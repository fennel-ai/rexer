import pickle
import sys
import base64
import pandas as pd
import rexerclient as rex

def run_transform(transform_obj, data):
    object_file = pickle.loads(base64.b64decode(transform_obj))
    df = pd.read_json(data)
    transformed_df = object_file(df)
    transformed_df = transformed_df.rename(columns={"Timestamp": "timestamp"})
    return transformed_df[['groupkey', 'value', 'timestamp']]

if __name__ == "__main__":
    try:
        output_df = run_transform(sys.argv[2], sys.argv[1])
        var = sys.stdout
        var.write(output_df.to_json(orient="records"))
    except Exception as e:
        print(e)
        sys.exit(3)