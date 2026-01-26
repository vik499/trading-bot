const MS_PER_SECOND = 1000;
const MS_PER_MINUTE = 60 * MS_PER_SECOND;
const MS_PER_HOUR = 60 * MS_PER_MINUTE;
const MS_PER_DAY = 24 * MS_PER_HOUR;

type UtcParts = {
    year: number;
    month: number;
    day: number;
    hour: number;
    minute: number;
    second: number;
    millisecond: number;
};

function pad2(value: number): string {
    return `${value}`.padStart(2, '0');
}

function pad3(value: number): string {
    return `${value}`.padStart(3, '0');
}

function pad4(value: number): string {
    const sign = value < 0 ? '-' : '';
    const abs = Math.abs(value);
    return `${sign}${`${abs}`.padStart(4, '0')}`;
}

function civilFromDays(days: number): { year: number; month: number; day: number } {
    let z = days + 719468;
    const era = Math.floor(z / 146097);
    const doe = z - era * 146097;
    const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
    let year = yoe + era * 400;
    const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
    const mp = Math.floor((5 * doy + 2) / 153);
    const day = doy - Math.floor((153 * mp + 2) / 5) + 1;
    const month = mp + (mp < 10 ? 3 : -9);
    year += month <= 2 ? 1 : 0;
    return { year, month, day };
}

function splitUtc(ts: number): UtcParts {
    const days = Math.floor(ts / MS_PER_DAY);
    const msOfDay = ts - days * MS_PER_DAY;
    const { year, month, day } = civilFromDays(days);

    let remaining = msOfDay;
    const hour = Math.floor(remaining / MS_PER_HOUR);
    remaining -= hour * MS_PER_HOUR;
    const minute = Math.floor(remaining / MS_PER_MINUTE);
    remaining -= minute * MS_PER_MINUTE;
    const second = Math.floor(remaining / MS_PER_SECOND);
    const millisecond = remaining - second * MS_PER_SECOND;

    return { year, month, day, hour, minute, second, millisecond };
}

export function formatUtcDate(ts: number): string {
    const parts = splitUtc(ts);
    return `${pad4(parts.year)}-${pad2(parts.month)}-${pad2(parts.day)}`;
}

export function formatUtcTimestamp(ts: number): string {
    const parts = splitUtc(ts);
    return `${pad4(parts.year)}-${pad2(parts.month)}-${pad2(parts.day)}T${pad2(parts.hour)}-${pad2(parts.minute)}-${pad2(parts.second)}.${pad3(parts.millisecond)}Z`;
}
