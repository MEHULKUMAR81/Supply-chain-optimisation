import pandas as pd
from pulp import LpProblem, LpMinimize, LpVariable, lpSum, LpBinary, LpStatus, value

df = pd.read_csv(
    r"C:\Users\ishum\OneDrive\Desktop\sql supply chain\supply chain LP\supply chain LP\ss.csv",
    low_memory=False
)

df.columns = df.columns.str.strip().str.lower()
df = df.drop_duplicates().reset_index(drop=True)

df["total_cost"] = pd.to_numeric(df["total_cost"], errors="coerce")
df["unit_qt"] = pd.to_numeric(df["unit_qt"], errors="coerce")
df["daily_capacity"] = pd.to_numeric(df["daily_capacity"], errors="coerce")
df["order_dt"] = pd.to_datetime(df["order_dt"], errors="coerce")

df = df.dropna(
    subset=["order_id", "plant_code", "order_dt", "unit_qt", "daily_capacity", "total_cost"]
).reset_index(drop=True)

df.insert(0, "optionid", df.index)

options = df["optionid"].tolist()
cost = dict(zip(df["optionid"], df["total_cost"]))
quantity = dict(zip(df["optionid"], df["unit_qt"]))

optionid_orderid = df.groupby("order_id")["optionid"].apply(list).to_dict()

capacity_df = df[["plant_code", "order_dt", "daily_capacity"]].drop_duplicates()

capacity = {
    (row["plant_code"], row["order_dt"]): row["daily_capacity"]
    for _, row in capacity_df.iterrows()
}

options_by_plant_date = (
    df.groupby(["plant_code", "order_dt"])["optionid"]
    .apply(list)
    .to_dict()
)

print("No. of options:", len(options))
print("No. of orders:", len(optionid_orderid))
print("No. of plant-date capacity keys:", len(options_by_plant_date))

model = LpProblem("SupplyChainAssignment", LpMinimize)

X = LpVariable.dicts("X", options, cat=LpBinary)

model += lpSum(cost[k] * X[k] for k in options)

for order_id, option_list in optionid_orderid.items():
    model += lpSum(X[k] for k in option_list) == 1

for (plant_code, order_dt), option_list in options_by_plant_date.items():
    model += lpSum(quantity[k] * X[k] for k in option_list) <= capacity[(plant_code, order_dt)]

model.solve()

print("Status:", LpStatus[model.status])
print("Objective value:", value(model.objective))

selected_options = [k for k in options if X[k].value() == 1]

result_df = df[df["optionid"].isin(selected_options)].copy()

print("Selected rows:", result_df.shape[0])
print("Unique selected orders:", result_df["order_id"].nunique())
print("Total unique orders in input:", df["order_id"].nunique())

utilisation = (
    result_df.groupby(["plant_code", "order_dt"])["unit_qt"]
    .sum()
    .reset_index(name="assigned_qty")
)

utilisation = utilisation.merge(
    capacity_df,
    on=["plant_code", "order_dt"],
    how="left"
)

utilisation["utilisation_pct"] = (
    utilisation["assigned_qty"] / utilisation["daily_capacity"]
) * 100

utilisation["excess_load"] = utilisation["assigned_qty"] - utilisation["daily_capacity"]

result_df.to_csv("lp_final_assignment.csv", index=False)
utilisation.to_csv("lp_plant_utilisation.csv", index=False)

print("Files saved: lp_final_assignment.csv, lp_plant_utilisation.csv")

#checks for feasibility 



print("Total demand:", df["unit_qt"].sum())
print("Total capacity:", capacity_df["daily_capacity"].sum())


check = df.groupby(["plant_code","order_dt"])["unit_qt"].sum().reset_index()
check = check.merge(capacity_df, on=["plant_code","order_dt"])

check["excess"] = check["unit_qt"] - check["daily_capacity"]

print(check[check["excess"] > 0])
