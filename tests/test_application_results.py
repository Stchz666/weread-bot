import unittest
import signal
from argparse import Namespace
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from tests.helpers import load_weread_bot


class ApplicationResultTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.bot = load_weread_bot()

    async def test_combined_summary_waits_for_both_named_accounts(self):
        bot = self.bot
        config = bot.WeReadConfig(max_concurrent_users=2)
        config.users = [bot.UserConfig(name="Stchz"),
                        bot.UserConfig(name="木衫", start_delay_seconds=300)]
        app = object.__new__(bot.WeReadApplication)
        app.config = config
        app._shutdown_event = bot.asyncio.Event()
        finished_first = bot.asyncio.Event()
        release_second = bot.asyncio.Event()
        sent = AsyncMock()

        def make_manager(_config, user, **kwargs):
            async def run():
                if user.name == "Stchz":
                    finished_first.set()
                else:
                    await release_second.wait()
                return bot.SessionResult(
                    bot.SessionStatus.SUCCESS,
                    bot.ReadingSession(user_name=user.name,
                                       actual_duration_seconds=60,
                                       successful_reads=2),
                )
            manager = unittest.mock.MagicMock()
            manager.start_reading_session = run
            return manager

        with (
            patch.object(bot, "WeReadSessionManager", side_effect=make_manager),
            patch.object(bot, "interruptible_sleep", new=AsyncMock(return_value=True)) as sleep,
            patch.object(bot.NotificationService, "send_notification_async", new=sent),
        ):
            task = bot.asyncio.create_task(bot.WeReadApplication._run_multi_user_sessions(app))
            await finished_first.wait()
            await bot.asyncio.sleep(0)
            sent.assert_not_awaited()
            release_second.set()
            result = await task
        self.assertEqual(result.successful_users, 2)
        sleep.assert_awaited_once()
        self.assertEqual(sleep.await_args.args[0], 300)
        sent.assert_awaited_once()
        message = sent.await_args.args[0]
        self.assertIn("Stchz：成功", message)
        self.assertIn("木衫：成功", message)
        self.assertEqual(sent.await_args.kwargs["event"], bot.NotificationEvent.MULTI_USER_SUMMARY)

    def test_run_result_status_and_exit_codes(self):
        success = self.bot.RunResult(final_status="success", user_count=1)
        partial = self.bot.RunResult(
            final_status="partial_success",
            user_count=2,
            successful_users=1,
            failed_users=1,
        )
        failed = self.bot.RunResult(
            final_status="failed", user_count=1, failed_users=1
        )
        cancelled = self.bot.RunResult(
            final_status="cancelled", user_count=1, cancelled_users=1
        )

        self.assertEqual(success.exit_code, 0)
        self.assertEqual(partial.exit_code, 1)
        self.assertEqual(failed.exit_code, 1)
        self.assertEqual(cancelled.exit_code, 1)
        self.assertEqual(partial.to_summary_dict()["final_status"], "partial_success")
        self.assertEqual(cancelled.to_summary_dict()["cancelled_users"], 1)

    def test_aggregate_session_results(self):
        stats = self.bot.ReadingSession(successful_reads=1)
        results = [
            self.bot.SessionResult(self.bot.SessionStatus.SUCCESS, stats),
            self.bot.SessionResult(
                self.bot.SessionStatus.FAILED,
                self.bot.ReadingSession(failed_reads=1),
                self.bot.RuntimeErrorCategory.NETWORK,
                "network failed",
            ),
        ]

        result = self.bot.RunResult.from_session_results(results)

        self.assertEqual(result.final_status, "partial_success")
        self.assertEqual(result.successful_users, 1)
        self.assertEqual(result.failed_users, 1)
        self.assertEqual(result.failure_categories, {"network": 1})

    async def test_single_user_failed_session_is_not_success(self):
        config = self.bot.WeReadConfig()
        app = object.__new__(self.bot.WeReadApplication)
        app.config = config
        self.bot.WeReadApplication._instance = app
        session_result = self.bot.SessionResult(
            self.bot.SessionStatus.FAILED,
            self.bot.ReadingSession(user_name="default", failed_reads=5),
            self.bot.RuntimeErrorCategory.PROTOCOL,
            "too many failures",
        )
        fake_manager = unittest.mock.MagicMock()
        fake_manager.start_reading_session = AsyncMock(return_value=session_result)

        with patch.object(self.bot, "WeReadSessionManager", return_value=fake_manager):
            result = await self.bot.WeReadApplication._run_single_user_session(app)

        self.assertEqual(result.final_status, "failed")
        self.assertEqual(result.exit_code, 1)

    async def test_main_returns_run_result_exit_code(self):
        config = self.bot.WeReadConfig(curl_content="safe")
        args = Namespace(
            mode=None,
            config="unused.yaml",
            verbose=False,
            validate_config=False,
            dry_run=False,
            show_last_run=False,
        )
        fake_app = unittest.mock.MagicMock()
        fake_app.run = AsyncMock(
            return_value=self.bot.RunResult(
                final_status="failed", user_count=1, failed_users=1
            )
        )
        with (
            patch.object(self.bot, "parse_arguments", return_value=args),
            patch.object(
                self.bot,
                "ConfigManager",
                return_value=unittest.mock.MagicMock(config=config),
            ),
            patch.object(self.bot, "setup_logging"),
            patch.object(self.bot, "_validate_runtime_config"),
            patch.object(self.bot, "_validate_curl_configs", new=AsyncMock()),
            patch.object(self.bot, "WeReadApplication", return_value=fake_app),
            patch.object(self.bot, "requests", object()),
            patch.object(self.bot, "httpx", object()),
        ):
            exit_code = await self.bot.main()

        self.assertEqual(exit_code, 1)

    async def test_main_returns_130_for_keyboard_interrupt(self):
        config = self.bot.WeReadConfig(curl_content="safe")
        args = Namespace(
            mode=None,
            config="unused.yaml",
            verbose=False,
            validate_config=False,
            dry_run=False,
            show_last_run=False,
        )
        fake_app = unittest.mock.MagicMock()
        fake_app.run = AsyncMock(side_effect=KeyboardInterrupt)
        with (
            patch.object(self.bot, "parse_arguments", return_value=args),
            patch.object(
                self.bot,
                "ConfigManager",
                return_value=unittest.mock.MagicMock(config=config),
            ),
            patch.object(self.bot, "setup_logging"),
            patch.object(self.bot, "_validate_runtime_config"),
            patch.object(self.bot, "_validate_curl_configs", new=AsyncMock()),
            patch.object(self.bot, "WeReadApplication", return_value=fake_app),
            patch.object(self.bot, "requests", object()),
            patch.object(self.bot, "httpx", object()),
        ):
            exit_code = await self.bot.main()

        self.assertEqual(exit_code, 130)

    async def test_dry_run_validation_failure_does_not_send_notification(self):
        config = self.bot.WeReadConfig(curl_content="safe")
        args = Namespace(
            mode=None,
            config="unused.yaml",
            verbose=False,
            validate_config=False,
            dry_run=True,
            show_last_run=False,
        )
        notification_service = unittest.mock.MagicMock()
        notification_service.send_notification_async = AsyncMock()

        with (
            patch.object(self.bot, "parse_arguments", return_value=args),
            patch.object(
                self.bot,
                "ConfigManager",
                return_value=unittest.mock.MagicMock(config=config),
            ),
            patch.object(self.bot, "setup_logging"),
            patch.object(
                self.bot,
                "_validate_runtime_config",
                side_effect=self.bot.ConfigError("invalid"),
            ),
            patch.object(
                self.bot,
                "NotificationService",
                return_value=notification_service,
            ),
        ):
            with self.assertLogs(level="ERROR"):
                exit_code = await self.bot.main()

        self.assertEqual(exit_code, 1)
        notification_service.send_notification_async.assert_not_awaited()

    def test_signal_requests_shutdown_and_records_interrupt(self):
        with patch.object(self.bot.signal, "signal"):
            app = self.bot.WeReadApplication(self.bot.WeReadConfig())

        app._signal_handler(signal.SIGINT, None)

        self.assertTrue(app.is_shutdown_requested())
        self.assertEqual(app.shutdown_signal, signal.SIGINT)

    def test_application_shutdown_state_is_isolated_per_instance(self):
        with patch.object(self.bot.signal, "signal"):
            first = self.bot.WeReadApplication(self.bot.WeReadConfig())
            second = self.bot.WeReadApplication(self.bot.WeReadConfig())

        first._signal_handler(signal.SIGTERM, None)

        self.assertTrue(first.is_shutdown_requested())
        self.assertFalse(second.is_shutdown_requested())

    async def test_scheduled_runtime_error_continues_to_next_cron(self):
        config = self.bot.WeReadConfig(
            startup_mode="scheduled",
            schedule=self.bot.ScheduleConfig(
                enabled=True,
                cron_expression="* * * * *",
                timezone="UTC",
            ),
        )
        app = object.__new__(self.bot.WeReadApplication)
        app.config = config
        app.execution_type = "normal"
        app._shutdown_event = self.bot.asyncio.Event()
        app.shutdown_signal = None
        self.bot.WeReadApplication._instance = app
        real_datetime = self.bot.datetime

        class FakeDateTime(real_datetime):
            current = real_datetime(
                2026, 7, 20, 12, 0, tzinfo=self.bot.ZoneInfo("UTC")
            )

            @classmethod
            def now(cls, timezone=None):
                if timezone is None:
                    return cls.current.replace(tzinfo=None)
                return cls.current.astimezone(timezone)

        class FakeCron:
            calls = 0

            def get_next(self, _result_type):
                self.calls += 1
                if self.calls > 1:
                    app._shutdown_event.set()
                return FakeDateTime.current + timedelta(seconds=1)

        async def fake_sleep(seconds, is_cancelled):
            FakeDateTime.current += timedelta(seconds=seconds)
            return not is_cancelled()

        session_attempt = AsyncMock(side_effect=RuntimeError("boom"))
        with (
            patch.object(self.bot, "datetime", FakeDateTime),
            patch.object(self.bot, "croniter", return_value=FakeCron()),
            patch.object(self.bot, "interruptible_sleep", new=fake_sleep),
            patch.object(
                self.bot.WeReadApplication,
                "run_single_session",
                new=session_attempt,
            ),
            patch.object(self.bot, "persist_run_history") as persist_history,
        ):
            with self.assertLogs(level="WARNING"):
                result = await app._run_scheduled_mode()

        self.assertEqual(result.final_status, "failed")
        self.assertEqual(result.failed_users, 0)
        self.assertEqual(result.failure_categories, {"unknown": 1})
        self.assertEqual(session_attempt.await_count, 1)
        persist_history.assert_called_once()

    async def test_daemon_runtime_error_waits_and_persists(self):
        config = self.bot.WeReadConfig(
            startup_mode="daemon",
            daemon=self.bot.DaemonConfig(
                enabled=True,
                session_interval="2",
                max_daily_sessions=3,
            ),
        )
        app = object.__new__(self.bot.WeReadApplication)
        app.config = config
        app.execution_type = "normal"
        app._shutdown_event = self.bot.asyncio.Event()
        app.shutdown_signal = None
        self.bot.WeReadApplication._instance = app
        self.bot.WeReadApplication._daily_session_count = 0
        self.bot.WeReadApplication._last_session_date = None
        sleep_calls = []

        async def session_attempt():
            if session_mock.await_count == 1:
                raise RuntimeError("boom")
            app._shutdown_event.set()
            return self.bot.RunResult(
                final_status="cancelled",
                user_count=1,
                cancelled_users=1,
            )

        async def fake_sleep(seconds, _is_cancelled):
            sleep_calls.append(seconds)
            app._shutdown_event.set()
            return False

        session_mock = AsyncMock(side_effect=session_attempt)
        with (
            patch.object(
                self.bot.WeReadApplication,
                "run_single_session",
                new=session_mock,
            ),
            patch.object(self.bot, "interruptible_sleep", new=fake_sleep),
            patch.object(self.bot, "persist_run_history") as persist_history,
        ):
            with self.assertLogs(level="WARNING"):
                result = await app._run_daemon_mode()

        self.assertEqual(result.final_status, "failed")
        self.assertEqual(result.failed_users, 0)
        self.assertEqual(result.failure_categories, {"unknown": 1})
        self.assertEqual(session_mock.await_count, 1)
        self.assertEqual(sleep_calls, [120])
        persist_history.assert_called_once()


if __name__ == "__main__":
    unittest.main()
