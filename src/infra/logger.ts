// Централизованный модуль логирования торгового бота.
// Выводит информационные и ошибочные сообщения с отметкой времени.
// В будущем можно расширить: запись логов в файл, уровни логов, отправка алертов.

const timestamp = (): string => {
    return new Date().toLocaleString();
};

export const logger = {
    info: (msg: string): void => {
        console.log(`[${timestamp()}] INFO: ${msg}`);
    },
    error: (msg: string, err?: unknown): void => {
        if (err instanceof Error) {
            const shortStack = err.stack?.split('\n').slice(0, 3).join('\n');
            console.error(`[${timestamp()}] ERROR: ${msg}\n${shortStack}`);
        } else {
            console.error(`[${timestamp()}] ERROR: ${msg}`);
        }
    }
}