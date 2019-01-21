open_tex = """\\begin{table}[H]
\centering
\\begin{tabular}{rcc}
\\toprule
 & \multicolumn{1}{c}{Speed} & \multicolumn{1}{c}{Throughput}  \\\\\n"""
close_tex = """\\bottomrule
\end{tabular}
\caption{Performance Report}
\label{performance}
\end{table}"""

import glob

import numpy as np
import pandas as pd

bench_found = []
lines = []
for benchmark in glob.glob("./results/OK/*"):
    benchmark_name = benchmark.split("\\")[-1]
    lines.append(["\midrule\n"])
    for deadline in glob.glob(benchmark + "/*/"):
        if "old" not in deadline and "error" not in deadline:
            line = []
            deadline_value = deadline.split("\\")[-2].replace("%", "\%")
            line.append(benchmark_name + " " + deadline_value)
            datas = []
            for error_file in glob.glob(deadline + "/*/CPU_TIME*"):
                data = pd.read_table(error_file, sep=" ", header=None)
                print(data)
                datas.append([data[1][2], data[1][3]])
            print(datas)
            for err in np.array(datas).mean(axis=0):
                line.append("{0:.2f}".format(err))
            lines.append(line)

print(lines)

with open("perfomance.tex", "w") as output_table:
    output_table.write(open_tex)
    for line in lines:
        if "midrule" in line[0]:
            out_line = line[0]
        else:
            out_line = " & ".join(line) + "\\\\\n"
        output_table.write(out_line)
    output_table.write(close_tex)
