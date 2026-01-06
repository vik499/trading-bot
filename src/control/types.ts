// Контрольные типы реэкспортируются из центрального EventBus, чтобы не было дублирования контрактов.
import type {
  BotMode,
  ControlCommand,
  ControlState,
  EventMeta,
  EventSource,
} from '../core/events/EventBus';

export type { BotMode, ControlCommand, ControlState, EventMeta, EventSource };

// Удобный алиас для типа команды
export type ControlCommandType = ControlCommand['type'];