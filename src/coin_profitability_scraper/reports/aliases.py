"""Store aliases for names across datasets."""

ALGORITHM_ALIASES: dict[str, list[str]] = {
    "Blake2B": ["Blake2b", "Blake (2b)"],
    "Blake2B-Sia": ["Blake2b-Sia", "Blake (2b)-Sia", "Blake (2b-Sia)"],
    "CryptoNight-Heavy": ["Cryptonight Heavy"],
    "CryptoNight-Lite": ["Cryptonight Lite", "CryptoNight-lite"],
    "CryptoNight-V7": ["Cryptonight V7", "CryptoNight V7", "CryptoNightV7"],
    "CuckooCycle": ["Cuckoo Cycle"],
    "Equihash(144,5)": ["Equihash 144,5", "Equihash1445", "Equihash 144_5"],
    "Equihash(192,7)": ["Equihash 192,7", "Equihash1927", "Equihash 192_7"],
    "Equihash(210,9)": ["Equihash210,9"],
    "FiroPoW": ["Firo Pow", "FiroPoW", "FiroPow"],
    "Handshake": ["HandShake"],
    "KawPow": ["Kaw Pow", "KawPow", "KAWPOW"],
    "Keccak": ["KECCAK"],  # Maybe include "KECCAK-256 (SHA-3)"
    "KHeavyHash": ["kHeavyHash"],
    "LPoS": ["LPos", "LPoS", "LPOS"],
    "Lyra2Z": ["Lyra2z"],
    "M7M": ["m7m"],
    "NexaPoW": ["Nexa Pow", "NexaPow", "NEXAPOW", "NexaPow"],
    "PoD": ["POD"],
    "Quark": ["QUARK", "quark"],
    "QuBit": ["Qubit", "QUbit", "qubit"],
    # While not technically correct, alias all SHA-256 and SHA-256D together.
    # The datasources don't do a good enough job distinguishing any pure SHA256 coins.
    "SHA-256": ["SHA-256D", "SHA256", "SHA256D"],
    "SHA256DT": ["SHA256dT", "SHA-256dT"],  # Note: Different than SHA-256D.
    "VerusHash": ["Verus hash", "VerusHash", "Verushash"],
    "XEVAN": ["Xevan"],
    "yescrypt": ["Yescript", "YesCript", "yescript", "Yescrypt"],  # Fixes typos.
    "XelisHash-V2": ["Xelishashv2"],
}

ALGORITHM_MAPPINGS: dict[str, str] = {  # Keys: aliases, Values: standard names.
    alias: standard_name
    for standard_name, aliases in ALGORITHM_ALIASES.items()
    for alias in aliases
}
