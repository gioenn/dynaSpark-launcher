open_tex = """\\begin{table}[h]
\caption{Deadline Errors Report}
\label{derror}
\\begin{tabular}{rccc}
\\toprule

 & \multicolumn{1}{c}{App} & \multicolumn{1}{c}{Stages} & \multicolumn{1}{c}{Stages} \\\\
 & \multicolumn{1}{c}{Error} & \multicolumn{1}{c}{Mean Error} & \multicolumn{1}{c}{DevStd Error} \\\\
\midrule\n"""
close_tex = """\\bottomrule
\end{tabular}
\end{table}"""

import glob

import numpy as np
import pandas as pd

bench_found = []
lines = []
for benchmark in glob.glob("./results/OK/*"):
    benchmark_name = benchmark.split("\\")[-1]
    for deadline in glob.glob(benchmark + "/*%"):
        line = []
        deadline_value = deadline.split("\\")[-1].replace("%", "\%")
        line.append(benchmark_name + " " + deadline_value)
        datas = []
        for error_file in glob.glob(deadline + "/*/ERROR*"):
            data = pd.read_table(error_file, sep=" ", header=None)
            datas.append(data[1][:3])
        for err in np.array(datas).mean(axis=0):
            line.append("{0:.2f}".format(err))
        lines.append(line)

print(lines)

with open("table.tex", "w") as output_table:
    output_table.write(open_tex)
    for line in lines:
        output_table.write(" & ".join(line) + "\\\\\n")
    output_table.write(close_tex)
