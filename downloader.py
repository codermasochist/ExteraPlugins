import os
import uuid
import time
import requests
import traceback
import threading
from settings import Header, Input, Divider, Switch, Selector
from base_plugin import BasePlugin, HookResult, HookStrategy
from android_utils import log, run_on_ui_thread
from java.io import File
from client_utils import show_error_bulletin, get_account_instance, get_media_data_controller, \
    get_last_fragment
from org.telegram.messenger import ApplicationLoader, SendMessagesHelper
from org.telegram.ui.ActionBar import AlertDialog
from java.util import ArrayList
from org.telegram.tgnet import TLRPC
import mimetypes

__name__ = "Media Downloader"
__description__ = "Download and upload media (photo, video, audio) from Internet"
__icon__ = "sukiviblyadi_by_fStikBot/1"
__version__ = "1.6.4"
__id__ = "downloader"
__author__ = "@itsv1eds"
__min_version__ = "11.9.0"

DEFAULT_COBALT_API = "https://co.itsv1eds.ru"
CUSTOM_COBALT_API = None
TEMP_DIR_NAME = "cobalt_temp"

progress_dialog = None


class VideoDownloaderPlugin(BasePlugin):
    def __init__(self):
        super().__init__()
        self._temp_dir = None

    def _socks_create_connection(self, dest_host, dest_port):
        # Minimal SOCKS5 connect handshake
        sock = __import__('socket').socket(__import__('socket').AF_INET, __import__('socket').SOCK_STREAM)
        # parse proxy_url and auth
        url = self.get_setting('proxy_url','').strip()
        user = self.get_setting('proxy_username','').strip()
        pwd = self.get_setting('proxy_password','').strip()
        if '://' in url:
            url = url.split('://',1)[1]
        phost, pport = url.split(':',1)
        sock.connect((phost, int(pport)))
        # greeting
        methods = b'\x02' if user and pwd else b'\x00'
        sock.send(b'\x05' + b'\x01' + methods)
        ver, nm = sock.recv(2)
        # auth
        if nm == 2:
            data = b'\x01' + bytes([len(user)]) + user.encode() + bytes([len(pwd)]) + pwd.encode()
            sock.send(data)
            sock.recv(2)
        # request connect
        addr = dest_host.encode()
        req = b'\x05\x01\x00\x03' + bytes([len(addr)]) + addr + int(dest_port).to_bytes(2,'big')
        sock.send(req)
        sock.recv(4)
        # skip remaining
        return sock

    def on_plugin_load(self):
        self._temp_dir = self._get_temp_dir()
        if self._temp_dir:
            log("VideoDownloaderPlugin loaded")
            self._cleanup_old_files()
        else:
            log("Failed to initialize temp directory")

    def create_settings(self):
        from java.util import Locale
        lang = Locale.getDefault().getLanguage()

        if lang.startswith('ru'):
            api_title = "Настройки API"
            api_url_label = "Cobalt API URL"
            api_key_label = "API ключ"
            api_desc = "Укажите URL и API (если требуется) вашего Cobalt инстанса. Узнать больше: github.com/imputnet/cobalt"
            settings_title = "Настройки загрузки"
            usage_cmd = ".down/.dl [URL] - Скачивает и отправляет медиа\nПример: .dl youtube.com/watch?v=dQw4w9WgXcQ"
            services_desc = "Поддерживаемые сервисы: YouTube, TikTok, Instagram, Twitter/X, VK (Видео/Клипы), Reddit, SoundCloud, Facebook, Pinterest, RuTube и другие."
            proxy_section_title = "Настройки прокси"
            proxy_type_label = "Тип прокси"
            proxy_type_items = ["Нет","HTTP","HTTPS","SOCKS"]
            proxy_url_text = "Прокси URL"
            proxy_user_text = "Логин прокси"
            proxy_pass_text = "Пароль прокси"
        else:
            api_title = "API Settings"
            api_url_label = "Cobalt API URL"
            api_key_label = "API Key"
            api_desc = "Enter URL and API (if needed) for your Cobalt instance. Learn more: github.com/imputnet/cobalt"
            settings_title = "Download Settings"
            usage_cmd = "Command: .down/.dl [URL] - Download and send video/audio\nExample: .dl youtube.com/watch?v=dQw4w9WgXcQ"
            services_desc = "Supported services: YouTube, TikTok, Instagram, Twitter/X, VK (Video/Clips), Reddit, SoundCloud, Facebook, Pinterest, RuTube and others"
            proxy_section_title = "Proxy Settings"
            proxy_type_label = "Proxy Type"
            proxy_type_items = ["None","HTTP","HTTPS","SOCKS"]
            proxy_url_text = "Proxy URL"
            proxy_user_text = "Proxy Username"
            proxy_pass_text = "Proxy Password"
        return [
            Header(text=api_title),
            Input(key="api_url", text=api_url_label, default=CUSTOM_COBALT_API or DEFAULT_COBALT_API),
            Input(key="api_key", text=api_key_label, default=""),
            Divider(text=api_desc),
            Header(text=proxy_section_title),
            Selector(key="proxy_type", text=proxy_type_label, default=0, items=proxy_type_items),
            Input(key="proxy_url", text=proxy_url_text, default="", subtext="Введите прокси host:port" if lang.startswith('ru') else "Enter proxy host:port"),
            Input(key="proxy_username", text=proxy_user_text, default="", subtext="Необязательно" if lang.startswith('ru') else "Optional"),
            Input(key="proxy_password", text=proxy_pass_text, default="", subtext="Необязательно" if lang.startswith('ru') else "Optional"),
            Header(text=settings_title),
            Switch(key="include_source", text="Включить ссылку источника" if lang.startswith('ru') else "Include source link", default=True, subtext="Добавлять исходную ссылку" if lang.startswith('ru') else "Add source link"),
            Switch(key="send_as_file", text="Отправлять как файл" if lang.startswith('ru') else "Send as file", default=False, subtext="Скачанный контент будет отправлен как документ" if lang.startswith('ru') else "Downloaded media will be sent as document"),
            Selector(key="video_quality", text="Качество видео" if lang.startswith('ru') else "Video Quality", default=4, items=["144","240","360","480","720","1080","1440","2160","4320","max"]),
            Selector(key="download_mode", text="Режим загрузки" if lang.startswith('ru') else "Download Mode", default=0, items=["auto","audio","mute"]),
            Selector(key="audio_bitrate", text="Битрейт аудио" if lang.startswith('ru') else "Audio Bitrate", default=2, items=["64","96","128","192","256","320"]),
            Divider(text=usage_cmd),
            Divider(text=services_desc),
        ]

    def _get_temp_dir(self):
        try:
            base_dir = ApplicationLoader.getFilesDirFixed()
            if not base_dir:
                return None
            temp_dir = File(base_dir, TEMP_DIR_NAME)
            if not temp_dir.exists() and not temp_dir.mkdirs():
                return None
            return temp_dir
        except Exception as e:
            log(f"Error creating temp dir: {e}")
            return None

    def _cleanup_old_files(self, max_age_hours=12):
        try:
            now = time.time()
            max_age_seconds = max_age_hours * 3600
            for file in self._temp_dir.listFiles():
                if file.isFile() and now - file.lastModified() / 1000 > max_age_seconds:
                    file.delete()
        except Exception as e:
            log(f"Cleanup error: {e}")

    def _download_video(self, video_url, mode_override=None):
        try:
            api_url = self.get_setting("api_url", CUSTOM_COBALT_API or DEFAULT_COBALT_API).rstrip('/')
            api_key = self.get_setting("api_key", "").strip()
            proxy_type = self.get_setting("proxy_type", 0)
            proxy_url = self.get_setting("proxy_url", "").strip()
            proxy_user = self.get_setting("proxy_username", "").strip()
            proxy_pass = self.get_setting("proxy_password", "").strip()
            if proxy_type == 3:
                # patch for SOCKS5
                try:
                    m = __import__('requests').packages.urllib3.util.connection
                    m.create_connection = lambda addr, timeout=None, **kw: self._socks_create_connection(addr[0], addr[1])
                    log('SOCKS5 patched')
                except Exception as e:
                    log(f"SOCKS patch failed: {e}")
                proxies = None
            elif proxy_type in (1,2):
                if proxy_type == 1:
                    prefix = "http"
                elif proxy_type == 2:
                    prefix = "https"
                else:
                    prefix = None
                if prefix and proxy_url:
                    if "://" not in proxy_url:
                        proxy_url = f"{prefix}://{proxy_url}"
                    if proxy_user and proxy_pass:
                        parts = proxy_url.split("://", 1)
                        scheme, rest = parts[0], parts[1]
                        proxy_url = f"{scheme}://{proxy_user}:{proxy_pass}@{rest}"
                    proxies = {"http": proxy_url, "https": proxy_url}
            else:
                proxies = None
            quality_setting = self.get_setting("video_quality", 4)
            quality_items = ["144","240","360","480","720","1080","1440","2160","4320","max"]
            if isinstance(quality_setting, int) and 0 <= quality_setting < len(quality_items):
                video_quality = quality_items[quality_setting]
            else:
                video_quality = str(quality_setting)
            payload = {
                "url": video_url,
                "videoQuality": video_quality
            }
            mode_setting = self.get_setting("download_mode", 0)
            mode_items = ["auto","audio","mute"]
            if isinstance(mode_setting, int) and 0 <= mode_setting < len(mode_items):
                download_mode = mode_items[mode_setting]
            else:
                download_mode = str(mode_setting)
            payload["downloadMode"] = download_mode
            if mode_override in mode_items:
                payload["downloadMode"] = mode_override
            bitrate_setting = self.get_setting("audio_bitrate", 2)
            bitrate_items = ["64","96","128","192","256","320"]
            if isinstance(bitrate_setting, int) and 0 <= bitrate_setting < len(bitrate_items):
                audio_bitrate = bitrate_items[bitrate_setting]
            else:
                audio_bitrate = str(bitrate_setting)
            payload["audioBitrate"] = audio_bitrate

            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            if api_key:
                headers["Authorization"] = f"Api-Key {api_key}"

            log(f"Calling Cobalt API in background thread: {video_url}")
            log(f"Downloader request -> URL: {api_url}/, payload: {payload}, headers: {headers}")

            resp = requests.post(f"{api_url}/", json=payload, headers=headers, timeout=30, proxies=proxies)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")

            if status == "error":
                code = data.get("error", {}).get("code", "Unknown error")
                show_error_bulletin(f"Cobalt API error: {code}")
                return None
            if status == "picker":
                items = data.get("picker", [])
                if not items:
                    show_error_bulletin("No items to download")
                    return None
                item = items[0]
                direct_url = item.get("url")
                filename = item.get("filename")
            else:
                direct_url = data.get("url")
                filename = data.get("filename")

            if not direct_url:
                show_error_bulletin("Invalid Cobalt API response")
                return None

            filename = filename or f"video_{uuid.uuid4()}.mp4"
            file_path = File(self._temp_dir, filename).getAbsolutePath()

            log(f"Downloading video in background thread: {filename}")

            video_resp = requests.get(direct_url, stream=True, timeout=60, proxies=proxies)
            video_resp.raise_for_status()

            with open(file_path, "wb") as f:
                for chunk in video_resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            try:
                size = os.path.getsize(file_path)
                log(f"Downloaded file saved at: {file_path}, size: {size} bytes")
            except Exception as dbg_e:
                log(f"Debug error checking file: {dbg_e}")

            return file_path
        except Exception as e:
            self._dismiss_dialog()
            log(f"Download error in background thread: {e}\n{traceback.format_exc()}")
            show_error_bulletin(f"Download error: {e}")
            return None

    def _download_audio(self, audio_url):
        try:
            api_url = self.get_setting("api_url", CUSTOM_COBALT_API or DEFAULT_COBALT_API).rstrip('/')
            api_key = self.get_setting("api_key", "").strip()
            proxy_type = self.get_setting("proxy_type", 0)
            proxy_url = self.get_setting("proxy_url", "").strip()
            proxy_user = self.get_setting("proxy_username", "").strip()
            proxy_pass = self.get_setting("proxy_password", "").strip()
            if proxy_type == 3:
                try:
                    m = __import__('requests').packages.urllib3.util.connection
                    m.create_connection = lambda addr, timeout=None, **kw: self._socks_create_connection(addr[0], addr[1])
                    log('SOCKS5 patched')
                except Exception as e:
                    log(f"SOCKS patch failed: {e}")
                proxies = None
            elif proxy_type in (1,2):
                if proxy_type == 1:
                    prefix = "http"
                elif proxy_type == 2:
                    prefix = "https"
                else:
                    prefix = None
                if prefix and proxy_url:
                    if "://" not in proxy_url:
                        proxy_url = f"{prefix}://{proxy_url}"
                    if proxy_user and proxy_pass:
                        parts = proxy_url.split("://", 1)
                        scheme, rest = parts[0], parts[1]
                        proxy_url = f"{scheme}://{proxy_user}:{proxy_pass}@{rest}"
                    proxies = {"http": proxy_url, "https": proxy_url}
            else:
                proxies = None
            bitrate_setting = self.get_setting("audio_bitrate", 2)
            bitrate_items = ["64","96","128","192","256","320"]
            if isinstance(bitrate_setting, int) and 0 <= bitrate_setting < len(bitrate_items):
                audio_bitrate = bitrate_items[bitrate_setting]
            else:
                audio_bitrate = str(bitrate_setting)
            payload = {"url": audio_url, "downloadMode": "audio", "audioBitrate": audio_bitrate}
            headers = {"Accept":"application/json","Content-Type":"application/json"}
            if api_key:
                headers["Authorization"] = f"Api-Key {api_key}"
            resp = requests.post(f"{api_url}/", json=payload, headers=headers, timeout=30, proxies=proxies)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")
            if status == "error":
                code = data.get("error", {}).get("code", "Unknown error")
                show_error_bulletin(f"Cobalt API error: {code}")
                return None
            if status == "picker":
                items = data.get("picker", [])
                if not items:
                    show_error_bulletin("No items to download")
                    return None
                item = items[0]
                direct_url = item.get("url")
                filename = item.get("filename")
            else:
                direct_url = data.get("url")
                filename = data.get("filename")
            if not direct_url:
                show_error_bulletin("Invalid Cobalt API response")
                return None
            filename = filename or f"audio_{uuid.uuid4()}.mp3"
            file_path = File(self._temp_dir, filename).getAbsolutePath()
            media_resp = requests.get(direct_url, stream=True, timeout=60, proxies=proxies)
            media_resp.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in media_resp.iter_content(8192): f.write(chunk)
            return file_path
        except Exception as e:
            show_error_bulletin(f"Audio download error: {e}")
            return None

    def _delete_file_delayed(self, path, delay=60):
        def action():
            try:
                time.sleep(delay)
                if os.path.exists(path):
                    os.remove(path)
                    log(f"Deleted temp file: {path}")
            except Exception as e:
                log(f"Delayed delete error: {e}")

        threading.Thread(target=action, daemon=True).start()

    def send_video(
            self,
            video_path: str,
            dialog_id: int,
            caption: str = None,
            ttl: int = 0,
            notify: bool = True,
            schedule_date: int = 0,
            force_document: bool = False,
            has_media_spoilers: bool = False,
            video_edited_info: object = None,
            cover_path: str = None,
            quick_reply_shortcut: str = None,
            quick_reply_shortcut_id: int = 0,
            effect_id: int = 0,
            stars: int = 0
    ):
        log(f"VideoDownloaderPlugin: send_video entry for path: {video_path}, dialog: {dialog_id} (from background thread)")
        ext = os.path.splitext(video_path)[1].lower()
        if ext in [".mp3", ".wav", ".ogg", ".opus", ".m4a"]:
            return self.send_audio(video_path, dialog_id, caption, notify, schedule_date)
        try:
            account_instance = get_account_instance()
            if account_instance is None:
                log("Error: Could not get AccountInstance.")
                return

            media_data_controller = get_media_data_controller()
            if media_data_controller is None:
                log("Error: Could not get MediaDataController.")
                return

            reply_message_object = None
            reply_top_message_object = None

            caption_entities_java = None
            if caption:
                caption_char_sequence_array = [caption]
                caption_entities_java = media_data_controller.getEntities(caption_char_sequence_array, True)
                
            SendMessagesHelper.prepareSendingVideo(
                account_instance,
                video_path,
                None,
                cover_path,
                None,
                dialog_id,
                reply_message_object,
                reply_top_message_object,
                None,
                None,
                caption_entities_java,
                ttl,
                None,
                notify,
                schedule_date,
                force_document,
                has_media_spoilers,
                caption,
                quick_reply_shortcut,
                quick_reply_shortcut_id,
                effect_id,
                stars
            )
            log(f"Video sending initiated for path: {video_path} to dialog: {dialog_id} (from background thread)")

        except Exception as e:
            log(f"Error preparing video for sending in background thread: {e}, type: {type(e)}")
            log(traceback.format_exc())
            show_error_bulletin(f"Error sending video: {e}")

    def send_audio(self, audio_path: str, dialog_id: int, caption: str = None, notify: bool = True, schedule_date: int = 0):
        log(f"VideoDownloaderPlugin: send_audio entry for path: {audio_path}, dialog: {dialog_id}")
        try:
            account_instance = get_account_instance()
            if account_instance is None:
                log("Error: Could not get AccountInstance.")
                return
            import mimetypes
            mime, _ = mimetypes.guess_type(audio_path)
            if mime is None:
                ext = os.path.splitext(audio_path)[1].lower()
                if ext == ".mp3":
                    mime = "audio/mpeg"
                elif ext == ".wav":
                    mime = "audio/wav"
                elif ext in [".ogg", ".opus", ".m4a"]:
                    mime = "audio/ogg"
                else:
                    mime = "application/octet-stream"
            ext_cache_root = ApplicationLoader.applicationContext.getExternalCacheDir()
            plugin_ext_dir = File(ext_cache_root, TEMP_DIR_NAME)
            if not plugin_ext_dir.exists() and not plugin_ext_dir.mkdirs():
                log("Failed to create external temp dir")
            external_path = File(plugin_ext_dir, File(audio_path).getName()).getAbsolutePath()
            with open(audio_path, 'rb') as f_in, open(external_path, 'wb') as f_out:
                while True:
                    chunk = f_in.read(8192)
                    if not chunk:
                        break
                    f_out.write(chunk)
            audio_path = external_path
            SendMessagesHelper.prepareSendingDocument(
                account_instance, audio_path, audio_path, None, caption, mime,
                dialog_id, None, None, None, None, None,
                notify, schedule_date, None, None, 0, False
            )
        except Exception as e:
            log(f"Error preparing audio for sending: {e}, type: {type(e)}")
            log(traceback.format_exc())
            show_error_bulletin(f"Error sending audio: {e}")

    def _process_download_and_send(self, url, dialog_id, notify, schedule_date, mode_override=None):
        try:
            video_path = self._download_video(url, mode_override)
            if video_path:
                if os.path.exists(video_path):
                    log(f"File exists, proceeding to send: {video_path}")
                    ext = os.path.splitext(video_path)[1].lower()
                    include_source = self.get_setting("include_source", True)
                    caption_text = f"Source: {url}" if include_source else None
                    account_instance = get_account_instance()
                    send_as_file = self.get_setting("send_as_file", False)
                    if send_as_file:
                        from java.util import Locale
                        if ext == ".gif":
                            lang = Locale.getDefault().getLanguage()
                            show_error_bulletin("Извини, я немощный, не могу отправить .gif" if lang.startswith('ru') else "Sorry, I'm powerless, I cannot send .gif")
                            self._delete_file_delayed(video_path)
                            self._dismiss_dialog()
                            return
                        import mimetypes
                        ext_cache_root = ApplicationLoader.applicationContext.getExternalCacheDir()
                        plugin_ext_dir = File(ext_cache_root, TEMP_DIR_NAME)
                        if not plugin_ext_dir.exists() and not plugin_ext_dir.mkdirs():
                            log("Failed to create external cache dir for document")
                        external_path = File(plugin_ext_dir, File(video_path).getName()).getAbsolutePath()
                        with open(video_path, 'rb') as f_in, open(external_path, 'wb') as f_out:
                            while True:
                                chunk = f_in.read(8192)
                                if not chunk:
                                    break
                                f_out.write(chunk)
                        mime, _ = mimetypes.guess_type(video_path)
                        if not mime:
                            mime = "application/octet-stream"
                        run_on_ui_thread(lambda: SendMessagesHelper.prepareSendingDocument(
                            account_instance, external_path, external_path, None,
                            caption_text, mime, dialog_id,
                            None, None, None, None, None,
                            notify, schedule_date, None, None, 0, False
                        ))
                        self._delete_file_delayed(video_path)
                        self._dismiss_dialog()
                        return
                    image_exts = [".jpg", ".jpeg", ".png"]
                    audio_exts = [".mp3", ".wav", ".ogg", ".opus", ".m4a"]
                    if ext in image_exts:
                        send_as_file = self.get_setting("send_as_file", False)
                        if send_as_file:
                            mime = "application/octet-stream"
                            run_on_ui_thread(lambda: SendMessagesHelper.prepareSendingDocument(
                                account_instance,
                                video_path,
                                video_path,
                                None,
                                caption_text,
                                mime,
                                dialog_id,
                                None, None, None, None, None,
                                notify, schedule_date, None, None, 0, False
                            ))
                            self._delete_file_delayed(video_path)
                            self._dismiss_dialog()
                            return
                        if include_source:
                            entities = ArrayList()
                            ent = TLRPC.TL_messageEntityTextUrl()
                            ent.offset = 0; ent.length = len("Source"); ent.url = url
                            entities.add(ent)
                            SendMessagesHelper.prepareSendingPhoto(
                                account_instance, video_path, None, dialog_id,
                                None, None, None, "Source", entities,
                                None, None, 0, None, notify, schedule_date,
                                0, None, 0
                            )
                        else:
                            SendMessagesHelper.prepareSendingPhoto(
                                account_instance, video_path, None, dialog_id,
                                None, None, None, None, None,
                                None, None, 0, None, notify, schedule_date,
                                0, None, 0
                            )
                    elif ext in audio_exts:
                        self.send_audio(video_path, dialog_id, caption_text, notify, schedule_date)
                    elif ext == ".gif":
                        from java.util import Locale
                        lang = Locale.getDefault().getLanguage()
                        show_error_bulletin("Извини, я немощный, не могу отправить .gif" if lang.startswith('ru') else "Sorry, I'm powerless, I cannot send .gif")
                        self._delete_file_delayed(video_path)
                        self._dismiss_dialog()
                        return
                    else:
                        if include_source:
                            entities = ArrayList()
                            ent = TLRPC.TL_messageEntityTextUrl()
                            ent.offset = 0; ent.length = len("Source"); ent.url = url
                            entities.add(ent)
                            SendMessagesHelper.prepareSendingVideo(
                                account_instance, video_path, None, None, None, dialog_id,
                                None, None, None, None, entities,
                                0, None, notify, schedule_date,
                                False, False, "Source", None, 0, 0, 0
                            )
                        else:
                            self.send_video(video_path, dialog_id, None, 0, notify, schedule_date)
                    self._delete_file_delayed(video_path)
                    self._dismiss_dialog()
                else:
                    log(f"Downloaded file not found after download: {video_path}")
                    show_error_bulletin(f"Internal error: File not found after download")
            else:
                log(f"Failed to download video for url: {url}")

        except Exception as e:
            self._dismiss_dialog()
            log(f"Error in _process_download_and_send: {e}\n{traceback.format_exc()}")
            show_error_bulletin(f"An unexpected error occurred: {e}")

    def _dismiss_dialog(self):
        global progress_dialog
        if progress_dialog is not None and progress_dialog.isShowing():
            try:
                progress_dialog.dismiss()
            except Exception:
                pass
            finally:
                progress_dialog = None

    def on_send_message_hook(self, account, params):
        if not hasattr(params, "message") or not isinstance(params.message, str):
            return HookResult()

        msg = params.message.strip()
        if msg.startswith(".down ") or msg.startswith(".up ") or msg.startswith(".dl "):
            parts = msg.split()
            mode_override = None
            if len(parts) > 2:
                mode_override = parts[2].lower()
            if len(parts) < 2 or not parts[1]:
                show_error_bulletin("No URL provided")
                return HookResult(strategy=HookStrategy.CANCEL)

            global progress_dialog
            progress_dialog = AlertDialog(get_last_fragment().getParentActivity(), 3)
            progress_dialog.show()

            url = parts[1].strip()
            dialog_id = params.peer
            notify = True
            schedule_date = 0

            log(f"Received command: {msg}. Starting background thread for URL: {url}")

            thread = threading.Thread(
                target=self._process_download_and_send,
                args=(url, dialog_id, notify, schedule_date, mode_override),
                daemon=True
            )
            thread.start()

            log("Background thread started. Cancelling original message.")
            return HookResult(strategy=HookStrategy.CANCEL)

        return HookResult()
