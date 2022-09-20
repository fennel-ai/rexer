import os
import pickle
import sys
import base64
from typing import Any, Dict, List, Optional, Union
import pandas as pd
import traceback
import inspect
import json
import rexerclient as rex

LOCAL_URL = "http://localhost:2425"


def run_transform(transform_obj: Any, args: str, types: str):
    os.environ["LOCAL_URL"] = LOCAL_URL
    object_file = pickle.loads(base64.b64decode(transform_obj))
    input = json.loads(args)
    type_dict = json.loads(types)
    for k,v in type_dict.items():
        if v == "<class 'pandas.core.frame.DataFrame'>":
            input[k] = pd.DataFrame.from_dict(input[k], orient='tight')

    os.environ["LOCAL_URL"] = LOCAL_URL
    transformed_df = object_file(**input)
    transformed_df = transformed_df.rename(columns={"Timestamp": "timestamp"})
    return transformed_df[['groupkey', 'value', 'timestamp']]


if __name__ == "__main__":
    try:
        output_df = run_transform(sys.argv[1], sys.argv[2], sys.argv[3])
        var = sys.stdout
        var.write(output_df.to_json(orient="records", date_unit='s'))
    except Exception as e:
        print(e)
        sys.exit(3)
