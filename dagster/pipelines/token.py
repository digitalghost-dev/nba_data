import toml


def get_motherduck_token() -> str:
    with open("../../secrets.toml") as f:
        secrets = toml.load(f)

    return secrets["tokens"]["motherduck"]
