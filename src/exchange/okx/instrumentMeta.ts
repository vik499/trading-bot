// OKX instrument metadata (local, no-network).
// Single source of truth for contract value (ctVal) used in swap trade normalization.

const OKX_SWAP_CT_VAL_BY_INST_ID: Record<string, number> = {
    'BTC-USDT-SWAP': 0.01,
};

export function getOkxSwapCtVal(instId: string): number | undefined {
    const key = instId.trim().toUpperCase();
    return OKX_SWAP_CT_VAL_BY_INST_ID[key];
}
