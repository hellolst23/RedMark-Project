import pandas as pd
import dask.dataframe as dd
from timeit import default_timer as timer

start = timer()
cdinfo = dd.read_csv('./data/cdinfo.txt', blocksize=100e6, names=['base', 'time', 'inorout', 'telephone'])

infected = pd.read_csv('./data/infected.txt', names=['telephone'])
infected_list = list(set(infected["telephone"].tolist()))
cdinfo_infected = cdinfo.loc[cdinfo["telephone"].isin(infected_list)]

# convert to pandas dataframe(because it is small dataset)
cdinfo_infected = cdinfo_infected.compute()
infected_base = list(set(cdinfo_infected["base"]))
infected_base.sort()

# If somebody has stayed in a polluted base station, it is judged as a red code
redmark_df = cdinfo.loc[cdinfo["base"].isin(infected_base)].compute()
redmark = list(set(redmark_df["telephone"].tolist()))
redmark.sort()

# superredmark:
infected_base_time = cdinfo_infected.groupby(["base", "telephone"])["time"].apply(list).reset_index(
    name='infected_time').drop(columns=["telephone"])

# For each polluted base station, record the corresponding polluted time period through a dictionary
# The reason for using a dictionary is to quickly find the value by key and avoid brute force search
infected_base_time_dict = {}
for index, row in infected_base_time.iterrows():
    if row["base"] not in infected_base_time_dict.keys():
        infected_base_time_dict[row["base"]] = [row["infected_time"]]
    else:
        infected_base_time_dict[row["base"]].append(row["infected_time"])

redmark_df = redmark_df.groupby(["base", "telephone"])["time"].apply(list).reset_index(name='stay_time')

# The following code is temporarily added in the formal experiment to solve the problem
# that the same person has been to the same base station twice
stay_time = redmark_df['stay_time'].tolist()
fmax = 0
for i in range(len(stay_time)):
    stay_time[i].sort()
    if len(stay_time[i]) >= fmax:
        fmax = len(stay_time[i])
print("fmax", fmax)
# fmax=4, which means there is a person entering and leaving the same base station twice

# new df from the column of lists
split_redmark_df = pd.DataFrame(stay_time, columns=['start', 'end','start1', 'end1'])
# split_redmark_df = pd.DataFrame(redmark_df['stay_time'].tolist(),columns=['start','end'])

# connect two dataframes
redmark_df = pd.concat([redmark_df, split_redmark_df], axis=1).drop(columns=["stay_time"])
# print(redmark_df)
is_infected = []
# Determine whether each person marked with a red code can be marked as a superredmark person line by line
for index, row in redmark_df.iterrows():
    flag = False
    for i in infected_base_time_dict[row["base"]]:
        if i[1] >= row["start"] and i[0] <= row["end"]:
            flag = True
        if i[1] >= row["start1"] and i[0] <= row["end1"]:
            flag = True
        # if i[1] >= row["start2"] and i[0] <= row["end2"]:
        #     flag = True
    is_infected.append(flag)

# redmark_df["stay_time"][0]<=infected_base_time_dict[redmark_df["base"]][1] and redmark_df["stay_time"][
# 1]>=infected_base_time_dict[redmark_df["base"]][0]
superredmark_df = redmark_df.loc[is_infected]
final = list(set(superredmark_df["telephone"].tolist()))
final.sort()

# write to superredmark.txt
with open('./result/redmark.txt', 'w') as fp:
    for item in final:
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')
print("共运行时间" + f"{timer() - start}")