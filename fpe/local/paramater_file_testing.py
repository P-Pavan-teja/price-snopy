def load_params(param_file_path):
    """Reads .param file into a dictionary (key=value per line)."""
    params = {}
    with open(param_file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue  # skip blanks and comments
            if "=" in line:
                key, value = line.split("=", 1)
                params[key.strip()] = value.strip()
    return params

param_file = "/Users/pavanteja/data_engineering/obfuscation/python_files/parameters.param"
params = load_params(param_file)

log_file = params["log_file"]
output_file = params["output_file"]
dict_path = params["dict_path"]
encryption_key = params["encryption_key"]
decrypted_path = params["decrypted_path"]
source_path = params["source_path"]
source_sheet_path = params["source_sheet_path"]
output_file = params["output_file"]

print(log_file)
print(output_file)
