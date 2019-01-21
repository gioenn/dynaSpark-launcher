# aggregate-by-key 0\%

# figs/evaluation/aggbykey-overview-0.pdf



import glob

for x in glob.glob("D:/Dropbox/2017-icse-spark-control/figs/evaluation/*"):
    x = x.split("\\")[-1]
    split = x.split("-")
    if "overview" in x:
        print(
            """\subfigure[][\scriptsize\\textbf{Overview / """ + split[0] + " " + split[-1].replace(
                ".pdf",
                "") + "\%" + """}]{\includegraphics[width=0.49\columnwidth]{figs/evaluation/"""
            + x + "}}")
    if "worker" in x:
        print("""\subfigure[][\scriptsize\\textbf{Worker / """ + split[0] + " " + split[-2].replace(
            ".pdf", "") + "\%" + """}]{\includegraphics[width=0.49\columnwidth]{figs/evaluation/"""
              + x + "}}")
