import logging


class InstancePrefixLogger:
    class _PrefixFilter(logging.Filter):
        def __init__(self, prefix: str):
            super().__init__()
            self._prefix = prefix

        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.msg
                if isinstance(msg, str):
                    if not msg.startswith(self._prefix):
                        record.msg = f"{self._prefix} {msg}"
                else:
                    record.msg = f"{self._prefix} {msg}"
            except Exception:
                pass
            return True

    @classmethod
    def build_prefixed(cls, base_coin: str) -> logging.Logger:
        prefix = f"[Executor {base_coin}]"
        name = f"hummingbot.strategy_v2.executors.maker_hedge_single_executor.{base_coin}.{id(prefix)}"
        logger = logging.getLogger(name)
        if not any(isinstance(f, cls._PrefixFilter) and getattr(f, '_prefix', None) == prefix for f in getattr(logger, 'filters', [])):
            logger.addFilter(cls._PrefixFilter(prefix))
        return logger
