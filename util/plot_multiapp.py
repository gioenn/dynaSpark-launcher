import matplotlib
import matplotlib.patches as patches
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

LABEL_SIZE = 20
PLOT_PARAMETERS = {
    'axes.labelsize': LABEL_SIZE,  # fontsize for x and y labels (was 10)
    'axes.titlesize': 8,
    'font.size': LABEL_SIZE,  # was 10
    'legend.fontsize': LABEL_SIZE,  # was 10
    'xtick.labelsize': LABEL_SIZE,
    'ytick.labelsize': LABEL_SIZE,
}

matplotlib.rcParams.update(PLOT_PARAMETERS)

CPUTIME_KMEANS_20 = 5348
CPUTIME_PAGERANK_0 = 6160
CPUTIME_PAGERANK_40 = 6160
CPUTIME_KMEANS_0 = 5768

COLOR_PAGERANK = "red"
COLOR_KMEANS = "blue"
ALPHA = 0.35
AVG_ALLOCATION_KEAMNS_0 = 43.7
AVG_ALLOCATION_KEAMNS_20 = 33.63
AVG_ALLOCATION_PAGERANK_40 = 27.2

DEADLINE_PAGERANK_40 = 239.474
DEADLINE_KMEANS_0 = 140.000
DEADLINE_KMEANS_20 = 168.000

KMEANS_SUBMISSION = 10

font0 = FontProperties()
font = font0.copy()
font.set_family('sans-serif')
style = "normal"

fig1, (ax1, ax2) = plt.subplots(2, sharex=True, sharey=True, figsize=(16, 5), dpi=300)
ax1.add_patch(
    patches.Rectangle(
        (0, 0),  # (x,y)
        DEADLINE_PAGERANK_40,  # width
        AVG_ALLOCATION_PAGERANK_40,  # height
        color=COLOR_PAGERANK,
        alpha=ALPHA,
        label='PageRank',
        linewidth=0,
    )
)
ax1.text(DEADLINE_PAGERANK_40 / 2, AVG_ALLOCATION_PAGERANK_40 / 2, 'PageRank',
         horizontalalignment='center',
         verticalalignment='center',
         fontsize=LABEL_SIZE, color='black',
         fontproperties=font,
         )
ax1.add_patch(
    patches.Rectangle(
        (KMEANS_SUBMISSION, AVG_ALLOCATION_PAGERANK_40),  # (x,y)
        DEADLINE_KMEANS_20,  # width
        AVG_ALLOCATION_KEAMNS_20,  # height
        color=COLOR_KMEANS,
        alpha=ALPHA,
        label='KMeans',
        linewidth=0
    )
)
ax1.text((KMEANS_SUBMISSION + DEADLINE_KMEANS_20) / 2,
         AVG_ALLOCATION_KEAMNS_20 / 2 + AVG_ALLOCATION_PAGERANK_40, 'KMeans',
         horizontalalignment='center',
         verticalalignment='center',
         fontproperties=font)

ax1.set_xlim(0.0, 250)
ax1.set_ylim(0.0, AVG_ALLOCATION_PAGERANK_40 + AVG_ALLOCATION_KEAMNS_20)

max_core = AVG_ALLOCATION_PAGERANK_40 + AVG_ALLOCATION_KEAMNS_20
pagerank_duration = CPUTIME_PAGERANK_0 / max_core
kmeans_duration = CPUTIME_KMEANS_0 / max_core
ax2.add_patch(
    patches.Rectangle(
        (0, 0),  # (x,y)
        pagerank_duration,  # width
        max_core,  # height
        color=COLOR_PAGERANK,
        alpha=ALPHA,
        label='PageRank',
        linewidth=0
    )
)
ax2.text(pagerank_duration / 2,
         max_core / 2, 'PageRank',
         horizontalalignment='center',
         verticalalignment='center',
         fontsize=LABEL_SIZE, color='black', fontproperties=font, )

ax2.add_patch(
    patches.Rectangle(
        (pagerank_duration, 0),
        # (x,y)
        kmeans_duration,  # width
        max_core,  # height
        color=COLOR_KMEANS,
        alpha=ALPHA,
        label='KMeans',
        linewidth=0
    )
)

ax2.text(pagerank_duration + kmeans_duration / 2,
         max_core / 2, 'KMeans',
         horizontalalignment='center',
         verticalalignment='center',
         fontsize=LABEL_SIZE, color='black', fontproperties=font, )

trans_ax1 = ax1.get_xaxis_transform()  # x in data untis, y in axes fraction
ax1.annotate('', xy=(0, 1), xytext=(0, 1.05), horizontalalignment='center',
             arrowprops=dict(fc=COLOR_PAGERANK, ec=COLOR_PAGERANK, alpha=0.8, shrink=0.05),
             xycoords=trans_ax1)

ax1.annotate('', xy=(KMEANS_SUBMISSION, 1), xytext=(KMEANS_SUBMISSION, 1.05),
             horizontalalignment='center',
             arrowprops=dict(fc=COLOR_KMEANS, ec=COLOR_KMEANS, alpha=0.8, shrink=0.05),
             xycoords=trans_ax1)

ax1.text(DEADLINE_PAGERANK_40, max_core + 1, "D",
         horizontalalignment='center', color=COLOR_PAGERANK, alpha=0.8)

ax1.text(KMEANS_SUBMISSION + DEADLINE_KMEANS_20, max_core + 1, "D",
         horizontalalignment='center', color=COLOR_KMEANS, alpha=0.8)

trans_ax2 = ax2.get_xaxis_transform()  # x in data untis, y in axes fraction
ax2.annotate('', xy=(0, 1), xytext=(0, 1.05), horizontalalignment='left',
             arrowprops=dict(fc=COLOR_PAGERANK, ec=COLOR_PAGERANK, alpha=0.8, shrink=0.05),
             xycoords=trans_ax2)
ax2.annotate('', xy=(KMEANS_SUBMISSION, 1), xytext=(KMEANS_SUBMISSION, 1.05),
             horizontalalignment='center',
             arrowprops=dict(fc=COLOR_KMEANS, ec=COLOR_KMEANS, alpha=0.8, shrink=0.05),
             xycoords=trans_ax2)

ax2.set_xlim(0.0, 250)
ax2.set_ylim(0.0, max_core)
ax2.set_xlabel("Time [s]")
ax2.set_ylabel("Core [#]")
ax1.set_ylabel("AvgCore [#]")

ax1.axvline(x=KMEANS_SUBMISSION + DEADLINE_KMEANS_20, ymin=-1.2, ymax=1, c="black", linestyle="--",
            linewidth=1, dashes=(4, 2),
            zorder=1, clip_on=False)
ax2.axvline(x=KMEANS_SUBMISSION + DEADLINE_KMEANS_20, ymin=0, ymax=1.2, c="black", linestyle="--",
            linewidth=1, dashes=(4, 2),
            zorder=1, clip_on=False)

ax1.axvline(x=DEADLINE_PAGERANK_40, ymin=-1.2, ymax=1, c="black", linestyle="--", linewidth=1,
            dashes=(4, 2),
            zorder=1, clip_on=False)
ax2.axvline(x=DEADLINE_PAGERANK_40, ymin=0, ymax=1.2, c="black", linestyle="--", linewidth=1,
            dashes=(4, 2),
            zorder=1, clip_on=False)
# ax1.legend(loc="best", framealpha=1.0)
fig1.savefig('spark-multi-app.pdf', dpi=300, bbox_inches='tight')
