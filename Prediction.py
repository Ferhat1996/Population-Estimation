import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor

################################
# functions:
################################

def create_time(a, b, c):
    if a == 2:
        result = np.ceil(b / c) * c
    elif a == 1:
        result = np.floor(b / c) * c
    else:
        result = np.round(b / c) * c
    print(result)

################################
# variables:
################################

# Assuming the files are in the same directory as the script
cced = "Final/CCEd.csv"
pop = "Final/Pop_CCED.csv"
period = 25
begyear = 1400
endyear = 1900
floor_val = 0
frac = 0.8
cced_data = pd.read_csv(cced, sep=";", header=0)
pop_data = pd.read_csv(pop, header=0)


################################
# Run:
################################

# Both data have different year frames
cced_data["Year"] = cced_data["Year"].astype(int)
plt.hist(cced_data["Year"])
plt.show()

cced_data = cced_data[(cced_data["Year"] > begyear) & (cced_data["Year"] < endyear)]

cced_data["appt"] = 1

cced_data["TYPE"] = cced_data["Type"].str[:5]
print(cced_data.groupby(["TYPE", "appt"]).size())
cced_data = cced_data[cced_data["TYPE"] == "Appt"]

# Control for "quality of the data"
cced_data = cced_data.groupby("cced_id").apply(
    lambda x: x.assign(minyear=np.min(x["Year"]), maxyear=np.max(x["Year"]))
)
cced_data["datarange"] = cced_data["maxyear"] - cced_data["minyear"] + 1

# cced_data = cced_data[cced_data["datarange"] > 200]

cced_data["time"] = create_time(floor_val, cced_data["Year"], period)

cced_data_reset = cced_data.reset_index()
cced_collapse = cced_data_reset.groupby(['cced_id', 'time', 'datarange', 'minyear', 'maxyear'])['appt'].sum().reset_index()

################################
# Run (continued):
################################

# Pop data
plt.hist(pop_data["year"])
plt.show()

pop_data["time"] = create_time(floor_val, pop_data["year"], period)
pop_collapse = pop_data.groupby(["Pop_ID", "cced_id", "time", "lat", "lon"]).max()
pop_collapse.rename(columns={"pop": "pop_max"}, inplace=True)

# Merging data
data = pd.merge(pop_collapse, cced_collapse, on=["cced_id", "time"])

data_grouped = data.groupby("Pop_ID").apply(
    lambda x: x.assign(
        minyear=np.min(x["minyear"]),
        maxyear=np.min(x["maxyear"]),
        datarange=np.min(x["datarange"]),
    )
)

# Calculate DQ
data_grouped["DQ"] = create_time(0, data_grouped["datarange"], 100)

# Variable transformations
data_grouped["lnPop"] = np.log(data_grouped["pop_max"])
data_grouped["lnApp"] = np.log(data_grouped["appt"])
data_grouped["appt2"] = data_grouped["appt"] ** 2
data_grouped["lnApp2"] = data_grouped["lnApp"] ** 2
data_grouped["latlon"] = data_grouped["lat"] * data_grouped["lon"]
data_grouped["tt"] = data_grouped["Pop_ID"] * data_grouped["time"]

smp_size = int(frac * data_grouped.shape[0])
train_id = np.random.choice(data_grouped.index, size=smp_size, replace=False)

train = data_grouped.loc[train_id]
test = data_grouped.drop(train_id)

# Linear Regression models
model_ols_1 = LinearRegression().fit(
    train[["appt", "time", "lat", "lon", "latlon", "minyear", "maxyear", "DQ", "tt"]],
    train["pop_max"],
)
# Continue with other OLS models...
# Continue from the previous code block...

# Linear Regression models (continued)
model_ols_2 = LinearRegression().fit(
    train[["appt", "time", "lat", "lon", "latlon", "minyear", "maxyear", "DQ", "tt"]],
    train["pop_max"],
)
# Continue with other OLS models...

# Random Forest model
outcome_data = train[["lnPop"]]
train_data = train.drop(columns=["lnPop", "pop_max"])

rf_fit = RandomForestRegressor(n_estimators=500)
rf_fit.fit(train_data, outcome_data.values.ravel())

# Prediction
test["lnPop_RF"] = rf_fit.predict(test)
test["Pop_RF"] = np.exp(test["lnPop_RF"])

# Evaluation
test["msqe_RF"] = np.sqrt((test["pop_max"] - test["Pop_RF"]) ** 2)

# Within 100 / 200
test["Pred_100_RF"] = np.where(np.abs(test["Pop_RF"] - test["pop_max"]) < 100, 1, 0)
test["Pred_200_RF"] = np.where(np.abs(test["Pop_RF"] - test["pop_max"]) < 200, 1, 0)

# Print summary or save it to a file
summary_columns = [
    "pop_max", "Pop_OLS_ln", "Pop_RF", "msqe_lnols", "msqe_RF",
    "Pred_100_ln", "Pred_100_RF", "Pred_200_ln", "Pred_200_RF"
]
summary = test[summary_columns]
print(summary)
# summary.to_csv("summary.csv", index=False)
# Continue from the previous code block...

# Print summary or save it to a file
summary_columns = [
    "pop_max", "Pop_OLS_ln", "Pop_RF", "msqe_lnols", "msqe_RF",
    "Pred_100_ln", "Pred_100_RF", "Pred_200_ln", "Pred_200_RF"
]
summary = test[summary_columns]
print(summary)
# summary.to_csv("summary.csv", index=False)

# Graphical Evaluation
import seaborn as sns

plt.figure(figsize=(10, 6))
sns.regplot(data=test, x="time", y="pop_max", order=4, scatter_kws={"s": 10})
sns.regplot(data=test, x="time", y="Pop_OLS_ln", order=4, scatter_kws={"s": 10}, line_kws={"color": "black", "linestyle": "--"})
plt.legend(["pop_max", "Pop_OLS_ln"])
plt.xlabel("Time")
plt.ylabel("Population")
plt.title("Graphical Evaluation")
plt.show()

