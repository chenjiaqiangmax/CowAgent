# -*- coding: utf-8 -*-
"""
WePilot 通道: 把企微真人账号接入 CowAgent。

入站: 常驻线程连 WePilot 后端的 WebSocket /api/ws?since=<_id>(游标续传),
把消息事件(11041 文本等)映射成 ChatMessage, 走 ChatChannel 标准管线
(群白名单/@过滤/插件/agent/会话队列)。
出站: send() 把回复 POST 给 WePilot 的 /api/queue/enqueue 且 origin="agent",
拟人延迟、每日配额、发送回显确认闭环全部由 WePilot 队列执行 —— 本通道
自己绝不直接调微灵网关, 也不伪造"人工确认"标志。

WePilot 侧依赖(wecom 仓库 backend, 由本通道的配套改动提供):
- /api/queue/enqueue 接受 origin="agent": 不计人工确认, 但照计配额、
  拟人延迟与回显闭环; 破坏性指令(踢人/退群/删联系人/转让群主/解散群)
  对 agent 一律拒绝
- AUTO_REPLY 规则回复需关闭或白名单化, 否则同一条客户消息会被规则引擎
  和 agent 各回一次(客户收到两条)

配置(CowAgent config.json):
- wepilot_base_url: WePilot 后端地址, 默认 http://127.0.0.1:9000
- wepilot_self_id: 机器人自己的企微 wxid(必填)。没有它无法区分"自发消息
  的回显", 通道会自己回自己形成死循环, 所以未配置直接启动失败
- wepilot_nick_name: 机器人昵称(选填, 群聊 @ 剥离用)
"""
import json
import threading

import requests
import websocket  # websocket-client, CowAgent requirements 自带

from bridge.context import Context, ContextType
from bridge.reply import Reply, ReplyType
from channel.chat_channel import ChatChannel
from channel.chat_message import ChatMessage
from common.log import logger


class WepilotMessage(ChatMessage):
    """从 WePilot 事件构造的消息体。

    事件的 ``_id`` 就是会话内消息 id(WePilot 落库回填的自增序号), 同时是
    WS 游标与取消接口的 message_id。
    """

    def __init__(self, raw_event, ctype, content):
        super().__init__(raw_event)
        self.ctype = ctype
        self.content = content
        self.msg_id = str(raw_event.get("_id") or "")
        self.create_time = str(raw_event.get("_ts") or "")


class WepilotChannel(ChatChannel):
    # 企微可发图片/文件/视频; 语音回复暂不支持
    NOT_SUPPORT_REPLYTYPE = [ReplyType.VOICE]

    # 微灵消息事件类型 -> ContextType。只接 CowAgent 能处理的消息
    # (wecom backend/models.py MSG_EVENTS 的子集), 其余事件忽略
    _CTYPE_MAP = {
        11041: ContextType.TEXT,     # 文本
        11047: ContextType.SHARING,  # 链接卡片
        11050: ContextType.SHARING,  # 名片
    }

    # 回复投递指令号(与 WePilot SendQueue/端点契约一致)
    _CMD_TEXT = 11029   # data: conversation_id + content, 单聊/群聊通用
    _CMD_IMAGE = 11030  # data: file = 本机绝对路径

    def __init__(self):
        super().__init__()
        self._stop = threading.Event()
        self._since = 0     # WS 游标: 最后处理到的事件 _id, 重连续传不漏不重
        self._ws = None
        self.name = None     # 机器人昵称(群聊 @ 剥离用)
        self.user_id = None  # 机器人自身 wxid(自回显过滤用)

    # ---------- 配置 ----------
    def _base_url(self):
        return str(self.cfg("wepilot_base_url", "http://127.0.0.1:9000")).rstrip("/")

    def _ws_url(self):
        return (
            self._base_url()
            .replace("http://", "ws://", 1)
            .replace("https://", "wss://", 1)
            + "/api/ws"
        )

    # ---------- 入站 ----------
    def startup(self):
        self.name = self.cfg("wepilot_nick_name", "") or None
        self.user_id = self.cfg("wepilot_self_id", "") or None
        if not self.user_id:
            # 没有自身 id 就无法区分自发回显, 会形成"自己回自己"死循环——
            # 宁可响亮失败, 不静默降级
            self.report_startup_error(
                "wepilot_self_id 未配置: 机器人自己的企微 wxid 必填"
                "(WePilot 控制台登录卡片可见)"
            )
            return
        self.report_startup_success()
        self._ws_loop()

    def _ws_loop(self):
        """WS 主循环: 断线自动重连, since 游标续传。跑在渠道的守护线程里。"""
        while not self._stop.is_set():
            try:
                ws = websocket.create_connection(
                    f"{self._ws_url()}?since={self._since}", timeout=10
                )
                ws.settimeout(None)  # recv 长阻塞; stop() 通过 close 唤醒
                self._ws = ws
                try:
                    self._recv_loop(ws)
                finally:
                    self._ws = None
                    ws.close()
            except Exception as e:
                if self._stop.is_set():
                    return
                logger.warning(
                    f"[wepilot] WS 断开({e}), 5s 后重连(since={self._since})"
                )
                self._stop.wait(5)

    def _recv_loop(self, ws):
        while not self._stop.is_set():
            raw = ws.recv()
            if not raw:
                return
            try:
                event = json.loads(raw)
            except Exception:
                continue
            self._handle_event(event)

    def _handle_event(self, event):
        """把一条 WePilot 事件映射成 ChatMessage 并送入标准管线。

        补发与订阅可能重复(WePilot /api/ws 的设计如此, 前端也按 _id 去重),
        游标单调递增, 重复的在这里被跳过。
        """
        eid = event.get("_id") or 0
        if eid and eid <= self._since:
            return
        if eid:
            self._since = eid
        ctype = self._CTYPE_MAP.get(event.get("type"))
        if ctype is None:
            return  # 非消息事件(登录/心跳/回显确认等)不进 agent 管线
        d = event.get("data") or {}
        cid = d.get("conversation_id") or ""
        if not cid:
            return
        sender = d.get("sender") or d.get("sender_id") or ""
        if sender and sender == self.user_id:
            return  # 自发消息的回显, 丢掉 —— 否则 agent 会自己回自己
        text = d.get("text_content") or d.get("content") or ""
        if not text:
            return
        msg = WepilotMessage(event, ctype=ctype, content=text)
        msg.from_user_id = sender
        msg.from_user_nickname = (
            d.get("sender_nickname") or d.get("sender_name") or ""
        )
        msg.to_user_id = self.user_id
        msg.other_user_id = cid
        msg.is_group = cid.startswith("R:")
        # 事件里不一定带群名; 用会话 id 兜底 —— 群白名单按 R:xxx 匹配
        msg.other_user_nickname = d.get("conversation_name") or cid
        if msg.is_group:
            # 群消息: 实际发言人就是 sender
            msg.actual_user_id = sender
            msg.actual_user_nickname = msg.from_user_nickname
            at_list = d.get("at_list") or []
            msg.at_list = at_list
            msg.is_at = self.user_id in at_list
        # isgroup 必须随 kwargs 进 _compose_context: 群/单聊分流在那里完成
        context = self._compose_context(ctype, text, msg=msg, isgroup=msg.is_group)
        if context:
            self.produce(context)

    # ---------- 出站 ----------
    @staticmethod
    def _is_dryrun(context) -> bool:
        """演练注入的事件(_dryrun)沿链路传播: 它的回复在队列执行点同样被拦截。

        否则演练注入(如 /api/autoreply/simulate)经 agent 产生的回复会真实
        外发给被注入的会话 —— 演练就不安全了。
        """
        msg = context.get("msg") if context else None
        raw = getattr(msg, "_rawmsg", None) or {}
        return bool(raw.get("_dryrun"))

    def send(self, reply: Reply, context: Context):
        receiver = context.get("receiver") if context else None
        if not receiver:
            logger.warning("[wepilot] 回复缺少 receiver, 丢弃")
            return
        dryrun = self._is_dryrun(context)
        if reply.type == ReplyType.TEXT:
            self._enqueue(self._CMD_TEXT, {
                "conversation_id": receiver,
                "content": reply.content,
            }, dryrun=dryrun)
        elif reply.type == ReplyType.IMAGE_URL:
            path = self._download(str(reply.content))
            if path:
                self._enqueue(self._CMD_IMAGE, {"file": path}, dryrun=dryrun)
        elif reply.type in (ReplyType.INFO, ReplyType.ERROR):
            # 提示/报错也走文本, 让用户在企微里能看到失败原因
            self._enqueue(self._CMD_TEXT, {
                "conversation_id": receiver,
                "content": reply.content,
            }, dryrun=dryrun)
        else:
            logger.warning(f"[wepilot] 暂不支持的回复类型: {reply.type}")

    def _enqueue(self, cmd_type, data, dryrun=False):
        """把指令交给 WePilot 队列(origin=agent)。

        拟人延迟/配额/回显闭环都在 WePilot 队列里做; 人工确认不适用
        agent 出站, 由 origin 语义代替(WePilot 侧按 origin 校验)。
        dryrun=True 时队列执行点无条件拦截(演练注入的回复保持演练性质)。
        """
        body = {
            "type": cmd_type,
            "data": data,
            "confirmed": False,
            "origin": "agent",
            "dryrun": bool(dryrun),
        }
        try:
            r = requests.post(
                self._base_url() + "/api/queue/enqueue", json=body, timeout=10
            )
            if r.status_code >= 400:
                logger.error(
                    f"[wepilot] 入队失败 HTTP {r.status_code}: {r.text[:200]}"
                )
            else:
                logger.info(
                    f"[wepilot] 已入队 {cmd_type}: "
                    f"{json.dumps(data, ensure_ascii=False)[:100]}"
                )
        except Exception as e:
            logger.error(f"[wepilot] 入队异常: {e}")

    def _download(self, url):
        """把图片 URL 落到本机临时文件, 供 WePilot 媒体指令(11030)取用。"""
        import os
        import tempfile

        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            ext = (os.path.splitext(url)[1] or ".png")[:8]
            fd, path = tempfile.mkstemp(suffix=ext, prefix="wepilot_")
            with os.fdopen(fd, "wb") as f:
                f.write(r.content)
            return path
        except Exception as e:
            logger.error(f"[wepilot] 图片下载失败: {e}")
            return None

    def stop(self):
        self._stop.set()
        ws = self._ws
        if ws is not None:
            # recv 正长阻塞时由 close 唤醒, 线程随后看到 _stop 退出
            try:
                ws.close()
            except Exception:
                pass
