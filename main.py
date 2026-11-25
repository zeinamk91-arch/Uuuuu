import asyncio
import json
import os
from typing import Dict, List, Optional

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

OWNER_ID = 1445715309129367602
MASTER_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
TARGET_GUILD_ID = int(os.getenv("GUILD_ID", "0"))
WORKERS_FILE = "workers.json"
CONFIG_FILE = "msg_config.json"
MIN_INTERVAL = 0.2


def load_json(path: str, default: dict) -> dict:
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default, f, indent=2, ensure_ascii=False)
        return dict(default)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return dict(default)


def save_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def mask_url(url: str) -> str:
    if not url:
        return "غير مضبوط"
    if len(url) <= 18:
        return "***"
    return f"{url[:14]}...{url[-6:]}"


def mask_token(token: str) -> str:
    if len(token) <= 10:
        return "***"
    return f"{token[:6]}...{token[-4:]}"


def is_authorized_user(interaction: discord.Interaction) -> bool:
    return interaction.user.id == OWNER_ID


class WorkerManager:
    def __init__(self) -> None:
        self.clients: Dict[str, discord.Client] = {}
        self.tasks: Dict[str, asyncio.Task] = {}

    async def start_worker(self, token: str) -> str:
        key = mask_token(token)
        if token in self.clients and not self.clients[token].is_closed():
            return f"{key}: متصل بالفعل"

        intents = discord.Intents.default()
        intents.guilds = True
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready():
            print(f"[Worker Ready] {client.user} ({key})")

        async def runner():
            try:
                await client.start(token)
            except discord.LoginFailure:
                print(f"[Worker Error] توكن غير صالح ({key})")
            except Exception as exc:
                print(f"[Worker Error] ({key}): {exc}")

        self.clients[token] = client
        self.tasks[token] = asyncio.create_task(runner())
        return f"{key}: جاري التشغيل"

    async def stop_worker(self, token: str) -> str:
        key = mask_token(token)
        client = self.clients.pop(token, None)
        task = self.tasks.pop(token, None)
        if client and not client.is_closed():
            await client.close()
        if task and not task.done():
            task.cancel()
        return f"{key}: تم الإيقاف"

    async def sync_all(self, tokens: List[str]) -> List[str]:
        results = []
        active_set = set(tokens)
        for existing in list(self.clients.keys()):
            if existing not in active_set:
                results.append(await self.stop_worker(existing))
        for token in tokens:
            results.append(await self.start_worker(token))
        return results

    async def send_message_to_channel(self, channel_id: int, content: str) -> List[str]:
        logs: List[str] = []
        for token, client in self.clients.items():
            key = mask_token(token)
            if not client.is_ready():
                logs.append(f"{key}: غير جاهز")
                continue
            ch = client.get_channel(channel_id)
            if ch is None:
                try:
                    ch = await client.fetch_channel(channel_id)
                except Exception:
                    ch = None
            if not ch or not hasattr(ch, "send"):
                logs.append(f"{key}: لا يمكن الوصول للروم {channel_id}")
                continue
            try:
                await ch.send(content)
                logs.append(f"{key}: أرسل بنجاح")
            except discord.HTTPException as exc:
                logs.append(f"{key}: خطأ HTTP ({exc.status})")
        return logs


class MasterBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.worker_manager = WorkerManager()
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.sender_task: Optional[asyncio.Task] = None
        self.rr_index: int = 0

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession()

        if TARGET_GUILD_ID:
            guild_obj = discord.Object(id=TARGET_GUILD_ID)
            self.tree.copy_global_to(guild=guild_obj)
            await self.tree.sync(guild=guild_obj)
        else:
            await self.tree.sync()

        w_data = load_json(WORKERS_FILE, {"tokens": []})
        tokens = [t.strip() for t in w_data.get("tokens", []) if isinstance(t, str) and t.strip()]
        if tokens:
            await self.worker_manager.sync_all(tokens)

    async def close(self) -> None:
        if self.sender_task and not self.sender_task.done():
            self.sender_task.cancel()
        for t in list(self.worker_manager.clients.keys()):
            await self.worker_manager.stop_worker(t)
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
        await super().close()

    async def send_balanced_webhooks(
        self, webhooks: List[str], content: str, username: Optional[str] = None
    ) -> str:
        valid_hooks = [u.strip() for u in webhooks if isinstance(u, str) and u.strip()]
        if not valid_hooks:
            return "لا توجد روابط Webhook."

        if not self.http_session or self.http_session.closed:
            self.http_session = aiohttp.ClientSession()

        total = len(valid_hooks)
        for attempt in range(total):
            idx = (self.rr_index + attempt) % total
            url = valid_hooks[idx]
            try:
                wh = discord.Webhook.from_url(url, session=self.http_session)
                await wh.send(content=content, username=username or None)
                self.rr_index = (idx + 1) % total
                return f"Webhook #{idx + 1}: تم الإرسال"
            except discord.HTTPException as exc:
                if exc.status == 429:
                    retry_after = getattr(exc, "retry_after", 1.0)
                    print(f"[Rate Limit] Webhook #{idx + 1} ينتظر {retry_after}s")
                else:
                    print(f"[Webhook HTTP {exc.status} #{idx + 1}] {mask_url(url)}")
            except Exception as exc:
                print(f"[Webhook Error #{idx + 1}] {mask_url(url)}: {exc}")

        self.rr_index = (self.rr_index + 1) % total
        return "فشل الإرسال عبر Webhooks."

    async def sender_loop(self) -> None:
        default_cfg = {
            "enabled": False,
            "mode": "both",  # webhook | workers | both
            "webhooks": [],
            "username": "",
            "channel_id": 0,
            "message": "رسالة تلقائية",
            "interval": 1.0,
        }
        while True:
            cfg = load_json(CONFIG_FILE, default_cfg)
            if not cfg.get("enabled"):
                await asyncio.sleep(0.5)
                continue

            raw_interval = float(cfg.get("interval", 1.0))
            interval = max(MIN_INTERVAL, raw_interval)
            mode = str(cfg.get("mode", "both")).lower()
            content = str(cfg.get("message", "رسالة تلقائية"))
            username = str(cfg.get("username", "")) or None

            if mode in ("webhook", "both"):
                res = await self.send_balanced_webhooks(cfg.get("webhooks", []), content, username)
                print(f"[Loop {interval}s] {res}")

            if mode in ("workers", "both"):
                ch_id = int(cfg.get("channel_id", 0))
                if ch_id:
                    w_logs = await self.worker_manager.send_message_to_channel(ch_id, content)
                    print(f"[Loop Workers] {' | '.join(w_logs)}")

            await asyncio.sleep(interval)


bot = MasterBot()


@bot.event
async def on_ready():
    print(f"[Master Ready] {bot.user}")
    if bot.sender_task is None or bot.sender_task.done():
        bot.sender_task = asyncio.create_task(bot.sender_loop())


async def deny_if_unauthorized(interaction: discord.Interaction) -> bool:
    if not is_authorized_user(interaction):
        await interaction.response.send_message(
            "هذا الأمر متاح حصراً للمالك المحدد.",
            ephemeral=True,
        )
        return True
    return False


# --- أوامر البوتات الفرعية (Workers) ---

@bot.tree.command(name="worker_add", description="إضافة توكن بوت فرعي وتشغيله")
@app_commands.default_permissions()
@app_commands.describe(token="توكن البوت الفرعي")
async def worker_add(interaction: discord.Interaction, token: str):
    if await deny_if_unauthorized(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    w_data = load_json(WORKERS_FILE, {"tokens": []})
    tokens = [t.strip() for t in w_data.get("tokens", []) if isinstance(t, str) and t.strip()]
    clean = token.strip()
    if clean not in tokens:
        tokens.append(clean)
        save_json(WORKERS_FILE, {"tokens": tokens})
    status = await bot.worker_manager.start_worker(clean)
    await interaction.followup.send(f"تمت الإضافة: {status}", ephemeral=True)


@bot.tree.command(name="worker_remove", description="حذف توكن بوت فرعي وإيقافه")
@app_commands.default_permissions()
@app_commands.describe(token="توكن البوت الفرعي المراد حذفه")
async def worker_remove(interaction: discord.Interaction, token: str):
    if await deny_if_unauthorized(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    clean = token.strip()
    w_data = load_json(WORKERS_FILE, {"tokens": []})
    tokens = [t.strip() for t in w_data.get("tokens", []) if isinstance(t, str) and t.strip()]
    if clean in tokens:
        tokens.remove(clean)
        save_json(WORKERS_FILE, {"tokens": tokens})
    status = await bot.worker_manager.stop_worker(clean)
    await interaction.followup.send(f"تمت الإزالة: {status}", ephemeral=True)


@bot.tree.command(name="worker_status", description="عرض حالة البوتات الفرعية المسجلة")
@app_commands.default_permissions()
async def worker_status(interaction: discord.Interaction):
    if await deny_if_unauthorized(interaction):
        return
    w_data = load_json(WORKERS_FILE, {"tokens": []})
    tokens = [t.strip() for t in w_data.get("tokens", []) if isinstance(t, str) and t.strip()]
    if not tokens:
        await interaction.response.send_message("لا توجد توكنات بوتات فرعية مسجلة.", ephemeral=True)
        return
    lines = []
    for t in tokens:
        key = mask_token(t)
        c = bot.worker_manager.clients.get(t)
        state = "متصل" if (c and c.is_ready()) else "غير متصل"
        lines.append(f"• `{key}` — {state}")
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


# --- أوامر الويب هوك (Webhooks) ---

@bot.tree.command(name="webhook_add", description="إضافة رابط Webhook للتوزيع المتوازن")
@app_commands.default_permissions()
@app_commands.describe(url="رابط Webhook", username="اسم مرسل (اختياري)")
async def webhook_add(interaction: discord.Interaction, url: str, username: Optional[str] = None):
    if await deny_if_unauthorized(interaction):
        return
    cfg = load_json(CONFIG_FILE, {"webhooks": []})
    hooks = [u.strip() for u in cfg.get("webhooks", []) if isinstance(u, str) and u.strip()]
    clean = url.strip()
    if clean not in hooks:
        hooks.append(clean)
    cfg["webhooks"] = hooks
    if username is not None:
        cfg["username"] = username.strip()
    save_json(CONFIG_FILE, cfg)
    await interaction.response.send_message(f"تمت إضافة الويب هوك ({len(hooks)} إجمالي).", ephemeral=True)


@bot.tree.command(name="webhook_remove", description="حذف رابط Webhook حسب رقمه في القائمة")
@app_commands.default_permissions()
@app_commands.describe(index="رقم الرابط (يبدأ من 1)")
async def webhook_remove(interaction: discord.Interaction, index: int):
    if await deny_if_unauthorized(interaction):
        return
    cfg = load_json(CONFIG_FILE, {"webhooks": []})
    hooks = cfg.get("webhooks", [])
    if index < 1 or index > len(hooks):
        await interaction.response.send_message(f"الرقم غير صحيح. الحالي: {len(hooks)}", ephemeral=True)
        return
    removed = hooks.pop(index - 1)
    cfg["webhooks"] = hooks
    save_json(CONFIG_FILE, cfg)
    await interaction.response.send_message(f"تم حذف الرابط #{index} (`{mask_url(removed)}`).", ephemeral=True)


@bot.tree.command(name="webhook_list", description="عرض روابط Webhook المسجلة")
@app_commands.default_permissions()
async def webhook_list(interaction: discord.Interaction):
    if await deny_if_unauthorized(interaction):
        return
    cfg = load_json(CONFIG_FILE, {"webhooks": []})
    hooks = cfg.get("webhooks", [])
    if not hooks:
        await interaction.response.send_message("لا توجد روابط Webhook مضافة.", ephemeral=True)
        return
    lines = [f"{i + 1}. `{mask_url(u)}`" for i, u in enumerate(hooks)]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


# --- أوامر الإرسال ---

@bot.tree.command(name="msg_config", description="تحديد الرسالة والفاصل والوضع والروم")
@app_commands.default_permissions()
@app_commands.describe(
    message="نص الرسالة",
    interval="الفاصل الزمني بالثواني (مثال: 0.5 أو 1)",
    mode="webhook أو workers أو both",
    channel_id="معرف الروم للبوتات الفرعية",
)
async def msg_config(
    interaction: discord.Interaction,
    message: str,
    interval: float = 1.0,
    mode: str = "both",
    channel_id: Optional[str] = None,
):
    if await deny_if_unauthorized(interaction):
        return
    mode_clean = mode.strip().lower()
    if mode_clean not in ("webhook", "workers", "both"):
        await interaction.response.send_message("الوضع يجب أن يكون: `webhook` أو `workers` أو `both`.", ephemeral=True)
        return

    safe_interval = max(MIN_INTERVAL, float(interval))
    cfg = load_json(CONFIG_FILE, {})
    cfg["message"] = message
    cfg["interval"] = safe_interval
    cfg["mode"] = mode_clean
    if channel_id and channel_id.strip().isdigit():
        cfg["channel_id"] = int(channel_id.strip())
    save_json(CONFIG_FILE, cfg)

    await interaction.response.send_message(
        f"تم التحديث — الفاصل: `{safe_interval}s` | الوضع: `{mode_clean}`",
        ephemeral=True,
    )


@bot.tree.command(name="msg_toggle", description="تشغيل أو إيقاف الإرسال")
@app_commands.default_permissions()
@app_commands.describe(enabled="true للتشغيل أو false للإيقاف")
async def msg_toggle(interaction: discord.Interaction, enabled: bool):
    if await deny_if_unauthorized(interaction):
        return
    cfg = load_json(CONFIG_FILE, {})
    cfg["enabled"] = bool(enabled)
    save_json(CONFIG_FILE, cfg)
    state = "شغال" if cfg["enabled"] else "متوقف"
    await interaction.response.send_message(f"حالة الإرسال الآن: **{state}**", ephemeral=True)


@bot.tree.command(name="msg_status", description="عرض إعدادات الإرسال الحالية")
@app_commands.default_permissions()
async def msg_status(interaction: discord.Interaction):
    if await deny_if_unauthorized(interaction):
        return
    cfg = load_json(CONFIG_FILE, {})
    w_data = load_json(WORKERS_FILE, {"tokens": []})
    text = (
        f"• الحالة: `{'شغال' if cfg.get('enabled') else 'متوقف'}`\n"
        f"• الفاصل: `{cfg.get('interval', 1.0)}s`\n"
        f"• الوضع: `{cfg.get('mode', 'both')}`\n"
        f"• روم البوتات الفرعية: `{cfg.get('channel_id', 0)}`\n"
        f"• عدد البوتات الفرعية: `{len(w_data.get('tokens', []))}`\n"
        f"• عدد Webhooks: `{len(cfg.get('webhooks', []))}`\n"
        f"• الرسالة: {cfg.get('message', '')}"
    )
    await interaction.response.send_message(text, ephemeral=True)


def run_bot() -> None:
    if not MASTER_TOKEN:
        raise RuntimeError("يرجى ضبط DISCORD_TOKEN عبر: source env.sh")
    bot.run(MASTER_TOKEN)


if __name__ == "__main__":
    run_bot()
