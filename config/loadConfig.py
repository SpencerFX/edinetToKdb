from pathlib import Path
import os
import tomli


def load_config(config_path: str | None = None) -> dict:
    if config_path is None:
        config_file = Path(__file__).resolve().parent / "config.toml"
    else:
        config_file = Path(config_path).resolve()

    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with open(config_file, "rb") as f:
        config = tomli.load(f)

    config_dir = config_file.parent

    def resolve_from_config_dir(path_str: str) -> str:
        return str((config_dir / path_str).resolve())

    config["paths"]["project_root"] = resolve_from_config_dir(config["paths"]["project_root"])
    config["paths"]["working_dir"] = resolve_from_config_dir(config["paths"]["working_dir"])
    config["paths"]["translations_csv"] = resolve_from_config_dir(config["paths"]["translations_csv"])
    config["paths"]["output_dir"] = resolve_from_config_dir(config["paths"]["output_dir"])
    config["paths"]["raw_dir"] = resolve_from_config_dir(config["paths"]["raw_dir"])

    Path(config["paths"]["working_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["output_dir"]).mkdir(parents=True, exist_ok=True)
    Path(config["paths"]["raw_dir"]).mkdir(parents=True, exist_ok=True)

    env_var_name = config.get("api", {}).get("env_var_name", "EDINET_API_KEY")
    api_key = os.getenv(env_var_name)

    if not api_key:
        raise ValueError(f"Missing required environment variable: {env_var_name}")

    config["api"]["api_key"] = api_key
    return config