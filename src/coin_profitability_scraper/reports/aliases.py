"""Store aliases for names across datasets."""

import polars as pl

ALGORITHM_ALIASES: dict[str, list[str]] = {
    "Blake256": ["Blake 256", "Blake256", "BLAKE256"],
    "Blake2B": ["Blake2b", "Blake (2b)"],
    "Blake2B + SHA3": [
        "Blake2b Sha3",
        "Blake2b SHA3",
        "Blake (2b) + SHA3",
        "Blake2B SHA3",
    ],
    "Blake2S": ["Blake2s", "Blake (2s)"],
    "Blake2B-Sia": ["Blake2b-Sia", "Blake (2b)-Sia", "Blake (2b-Sia)"],
    "BMW512": ["BMW 512", "BMW512", "BMW-512", "bmw512", "Bmw512"],
    "CPUPower": ["CPU Power", "CPUpower"],
    "CryptoNight-Alloy": ["Cryptonight Alloy", "CryptoNight Alloy"],
    "CryptoNight-GPU": ["Cryptonight GPU", "CryptoNight GPU"],
    "CryptoNight-Heavy": ["Cryptonight Heavy", "CryptoNight Heavy"],
    "CryptoNight-Lite": ["Cryptonight Lite", "CryptoNight-lite", "CryptoNight Lite"],
    "CryptoNight-V7": ["Cryptonight V7", "CryptoNight V7", "CryptoNightV7"],
    "CuckooCycle": ["Cuckoo Cycle"],
    "Cuckaroo29s": ["Cuckaroo29S"],
    "Equihash Scrypt": ["Equihash-Scrypt", "EquihashScrypt", "Equihash+Scrypt"],
    "Equihash(125,4)": ["Equihash 125,4"],
    "Equihash(144,5)": ["Equihash 144,5", "Equihash1445", "Equihash 144_5"],
    "Equihash(150,5)": ["Equihash 150,5"],
    "Equihash(192,7)": ["Equihash 192,7", "Equihash1927", "Equihash 192_7"],
    "Equihash(210,9)": ["Equihash210,9", "Equihash 210,9"],
    "Equihash(96,5)": ["Equihash 96,5", "Equihash 96_5"],
    "FiroPoW": ["Firo Pow", "FiroPoW", "FiroPow"],
    "Handshake": ["HandShake"],
    "KarlsenHash": ["Karlsenhash"],
    "KarlsenHashV2": ["Karlsenhashv2", "KarlsenHash v2"],
    "KawPow": ["Kaw Pow", "KawPow", "KAWPOW"],
    "Keccak": ["KECCAK"],  # Maybe include "KECCAK-256 (SHA-3)"
    "Keccak-256 (SHA-3)": ["KECCAK-256 (SHA-3)"],  # May be the same as Keccak.
    "KHeavyHash": ["kHeavyHash"],
    "LPoS": ["LPos", "LPoS", "LPOS"],
    "Lyra2-MintMe": ["Lyra2 MintMe", "Lyra2 MintMe version", "Lyra2-MintMe version"],
    "Lyra2Z": ["Lyra2z"],
    "M7M": ["m7m"],
    "Mega-BTX": ["MegaBTX", "Mega BTX"],
    "Myriad-Groestl": ["Myr-Groestl", "Myriad Groestl"],
    "NexaPoW": ["Nexa Pow", "NexaPow", "NEXAPOW", "NexaPow", "NexaHash", "Nexahash"],
    "PoD": ["POD"],
    "Quark": ["QUARK", "quark"],
    "QuBit": ["Qubit", "QUbit", "qubit"],
    # While not technically correct, alias all SHA-256 and SHA-256D together.
    # The datasources don't do a good enough job distinguishing any pure SHA256 coins.
    "SHA-256": ["SHA-256D", "SHA256", "SHA256D", "SHA 256"],
    "SHA256DT": ["SHA256dT", "SHA-256dT"],  # Note: Different than SHA-256D.
    "Time Travel": ["Timetravel", "Time Travel", "TimeTravel"],
    "VerusHash": ["Verus hash", "VerusHash", "Verushash"],
    "XEVAN": ["Xevan"],
    "YesPower": ["YesPower", "Yespower", "YesPoWer"],
    "YesPowerR16": ["YesPoWerR16"],
    "Yescrypt": ["Yescript", "YesCript", "yescript", "Yescrypt"],  # Fixes typos.
    "X11GOST": ["X11 Gost", "X11Gost"],
    "XelisHash-V2": ["Xelishashv2"],
}

ALGORITHM_MAPPINGS: dict[str, str] = {  # Keys: aliases, Values: standard names.
    alias: standard_name
    for standard_name, aliases in ALGORITHM_ALIASES.items()
    for alias in aliases
}


def pre_mapping_normalize_algorithm_names(expr: pl.Expr) -> pl.Expr:
    """Do string replacement operations before dict mapping.

    Has unit test.
    """
    # CryptoNight mapping.
    expr = expr.str.replace(r"(?i)cryptonight[-_ ]?", "CryptoNight-").replace(
        {"CryptoNight-": "CryptoNight"}
    )

    # Equihash mapping.
    expr = expr.str.replace(
        r"(?i)equihash(?:[(, ]*)(\d+)[,_ ](\d+)(?:[), ]*)", r"Equihash(${1},${2})"
    )

    # Cuckatoo capitalization.
    expr = expr.str.replace_all(r"(?i)\b(cuckatoo)", "Cuckatoo")

    expr = expr.str.replace_all(r"(?i)\b(SHA 2)", "SHA-2")
    expr = expr.str.replace_all(r"(?i)\b(sha3)", "SHA3")

    return expr  # noqa: RET504


def post_mapping_normalize_algorithm_names(expr: pl.Expr) -> pl.Expr:
    """Do string replacement operations after dict mapping."""
    return expr


def normalize_algorithm_names(expr: pl.Expr) -> pl.Expr:
    """Do all column normalization operations on columns."""
    x = pre_mapping_normalize_algorithm_names(expr)
    x = x.replace(ALGORITHM_MAPPINGS)
    return post_mapping_normalize_algorithm_names(x)
