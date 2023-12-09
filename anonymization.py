import pandas as pd
import uuid

def AnonymizeAddress(aliases: dict, df: pd.DataFrame):
    for i, r in df.iterrows():
        alias = aliases[r["DirectorAddress"]]
        df.at[i, "DirectorAddress"] = alias
    return df

def AnonymizeName(aliases: dict, df: pd.DataFrame):
    for i, r in df.iterrows():
        alias = aliases[r["DirectorName"]]
        df.at[i, "DirectorName"] = alias
    return df

def ObscureCoordinates(df: pd.DataFrame):
    for i, r in df.iterrows():
        alias_lat = round(r["DirectorAddressLatitude"], 3)
        alias_lon = round(r["DirectorAddressLongitude"], 3)
        df.at[i, "DirectorAddressLatitude"] = alias_lat
        df.at[i, "DirectorAddressLongitude"] = alias_lon
    return df

def main():
    df = pd.read_csv("data_v6.csv")
    addressAliases = dict(zip([x for x in df["DirectorAddress"].unique()], [str(uuid.uuid4()) for x in df["DirectorAddress"].unique()]))
    nameAliases = dict(zip([x for x in df["DirectorName"].unique()], [str(uuid.uuid4()) for x in df["DirectorName"].unique()]))

    df = AnonymizeAddress(addressAliases, df)
    df = AnonymizeName(nameAliases, df)
    df = ObscureCoordinates(df)
    
    print(df[["DirectorAddress", "DirectorName", "DirectorAddressLatitude", "DirectorAddressLongitude"]])
    df.to_csv("data_v6_anonymized.csv", index=False)


if __name__ == "__main__":
    main()