import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline';
import { createMeta, eventBus, type RecoveryRequested, type SnapshotRequested } from '../core/events/EventBus';
import { logger } from '../infra/logger';
import { m } from '../core/logMarkers';
import type { ControlCommand, ControlState, EventMeta } from '../control/types';

type ControlCommandInput = ControlCommand extends infer U
  ? U extends { meta: EventMeta }
    ? Omit<U, 'meta'> & { meta?: EventMeta }
    : never
  : never;

function publish(cmd: ControlCommandInput) {
  const payload = { ...cmd, meta: cmd.meta ?? createMeta('cli') } as ControlCommand;
  eventBus.publish('control:command', payload);
}

function makeSink(rl: readline.Interface) {
  return (_entry: unknown, formatted: string) => {
    const line = rl.line;
    const cursor = rl.cursor; // позиция курсора в текущей строке

    try {
      readline.clearLine(process.stdout, 0);
      readline.cursorTo(process.stdout, 0);
      process.stdout.write(formatted + '\n');

      // prompt(true) сам перерисует prompt и текущий line из rl
      rl.prompt(true);

      // вернуть курсор на прежнюю позицию внутри строки
      const moveLeft = line.length - cursor;
      if (moveLeft > 0) {
        readline.moveCursor(process.stdout, -moveLeft, 0);
      }
    } catch {
      process.stdout.write(formatted + '\n');
    }
  };
}

export type CliStatusProvider = () => string[];

export function startCli(options: { logDir?: string; getStatusLines?: CliStatusProvider } = {}) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'bot> ',
  });

  let stopping = false;

  // Настраиваем sink, чтобы логи не портили prompt
  logger.setSink((entry, formatted) => {
    if (entry.level === 'error') {
      // Ошибки выводим в stderr, но тоже аккуратно, сохраняя ввод и курсор
      const line = rl.line;
      const cursor = rl.cursor;
      readline.clearLine(process.stdout, 0);
      readline.cursorTo(process.stdout, 0);
      process.stderr.write(formatted + '\n');
      if (!stopping) {
        rl.prompt(true);
        const moveLeft = line.length - cursor;
        if (moveLeft > 0) readline.moveCursor(process.stdout, -moveLeft, 0);
      }
      return;
    }

    if (stopping) {
      process.stdout.write(formatted + '\n');
      return;
    }

    makeSink(rl)(entry, formatted);
  });

  const stateHandler = (state: ControlState) => {
    // краткий вывод состояния
    logger.ui(
      `[CLI] Status: mode=${state.mode} paused=${state.paused} lifecycle=${state.lifecycle} uptime=${Math.floor(
        (Date.now() - state.startedAt) / 1000,
      )}s`,
    );
  };

  eventBus.subscribe('control:state', stateHandler);

  const logDir = options.logDir ?? process.env.LOG_DIR ?? 'logs';

  const printHelp = () => {
    logger.ui(
      'Commands: help | status | pause | resume | snapshot [path] | recover [path] | mode live|paper|backtest | logs on|off | logs tail <N> | level <debug|info|warn|error> | exit',
    );
  };

  printHelp();
  rl.prompt();

  rl.on('line', (line) => {
    if (stopping) return;

    const input = line.trim();
    logger.debug(m('ws', `CLI received line: "${line}"`));

    if (!input) return rl.prompt();

    const [cmd, ...rest] = input.split(/\s+/);
    const arg = rest[0];
    const arg2 = rest[1];

    logger.debug(`CLI parsed command: ${cmd}`);

    switch (cmd) {
      case 'status':
        publish({ type: 'status' });
        if (options.getStatusLines) {
          const lines = options.getStatusLines();
          lines.forEach((line) => logger.ui(line));
        }
        break;
      case 'help':
        printHelp();
        break;

      case 'pause':
        publish({ type: 'pause' });
        break;

      case 'resume':
        publish({ type: 'resume' });
        break;

      case 'snapshot': {
        const req: SnapshotRequested = {
          meta: createMeta('cli'),
          pathOverride: arg,
        };
        eventBus.publish('state:snapshot_requested', req);
        logger.info('[CLI] snapshot requested');
        break;
      }

      case 'recover': {
        const req: RecoveryRequested = {
          meta: createMeta('cli'),
          pathOverride: arg,
        };
        eventBus.publish('state:recovery_requested', req);
        logger.info('[CLI] recovery requested');
        break;
      }

      case 'mode': {
        const v = (arg ?? '').toLowerCase();
        if (v === 'live') publish({ type: 'set_mode', mode: 'LIVE' });
        else if (v === 'paper') publish({ type: 'set_mode', mode: 'PAPER' });
        else if (v === 'backtest') publish({ type: 'set_mode', mode: 'BACKTEST' });
        else logger.ui('usage: mode live|paper|backtest');
        break;
      }

      case 'logs': {
        const sub = (arg ?? '').toLowerCase();
        if (sub === 'on') {
          logger.setDisplay(true);
          logger.setConsoleMode('verbose');
          logger.ui('Консоль: расширенный режим включён');
        } else if (sub === 'off') {
          logger.setDisplay(true);
          logger.setConsoleMode('ui');
          logger.ui('Консоль: UI-режим, подробные логи скрыты');
        } else if (sub === 'tail') {
          const lineBefore = rl.line;
          const cursorBefore = rl.cursor;
          const n = Number.parseInt(arg2 ?? '', 10);
          const limit = Number.isFinite(n) ? n : 50;
          const errorsPath = path.join(logDir, 'errors.log');
          const warningsPath = path.join(logDir, 'warnings.log');
          const lines: Array<{ label: string; line: string }> = [];
          const tailFile = (filePath: string, label: string) => {
            if (!fs.existsSync(filePath)) return;
            const content = fs.readFileSync(filePath, 'utf8');
            const rows = content.split(/\r?\n/).filter((line) => line.trim().length > 0);
            const slice = rows.slice(-limit);
            slice.forEach((line) => lines.push({ label, line }));
          };
          tailFile(errorsPath, 'errors.log');
          tailFile(warningsPath, 'warnings.log');

          lines.forEach((entry) => {
            readline.clearLine(process.stdout, 0);
            readline.cursorTo(process.stdout, 0);
            process.stdout.write(`[${entry.label}] ${entry.line}\n`);
          });
          rl.prompt(true);
          const moveLeft = lineBefore.length - cursorBefore;
          if (moveLeft > 0) readline.moveCursor(process.stdout, -moveLeft, 0);
        } else {
          logger.ui('usage: logs on|off|tail <N>');
        }
        break;
      }

      case 'level': {
        const lvl = (arg ?? '').toLowerCase();
        if (lvl === 'debug' || lvl === 'info' || lvl === 'warn' || lvl === 'error') {
          logger.setLevel(lvl as any);
          logger.ui(`log level set to ${lvl}`);
        } else {
          logger.ui('usage: level debug|info|warn|error');
        }
        break;
      }

      case 'exit':
        publish({ type: 'shutdown', reason: 'cli exit' });
        stopping = true;
        rl.pause();
        rl.close();
        break;

      default:
        logger.ui(m('error', 'Unknown command. Use: status | pause | resume | snapshot [path] | recover [path] | mode live|paper|backtest | exit'));
    }

    rl.prompt();
  });

  rl.on('close', () => {
    eventBus.unsubscribe('control:state', stateHandler);
    logger.resetSinkToConsole();
  });

  // Если shutdown пришёл извне (другой контроллер), тоже закрываем prompt
  const shutdownHandler = (cmd: ControlCommand) => {
    if (cmd.type === 'shutdown') {
      stopping = true;
      rl.pause();
      rl.close();
    }
  };
  eventBus.subscribe('control:command', shutdownHandler);
}
