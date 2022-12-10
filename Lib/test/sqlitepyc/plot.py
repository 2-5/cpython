import numpy as np

from bokeh.plotting import figure, show

FS_TIMES = [100]
DB_TIMES = [100]

fs = FS_TIMES
db = DB_TIMES

time_max = max(max(fs), max(db))
bins = np.linspace(0, time_max + 10, 100)

p = figure(width=800, height=600)

fs_hist, fs_edges = np.histogram(fs, density=True, bins=bins)
p.quad(top=fs_hist, bottom=0, left=fs_edges[:-1], right=fs_edges[1:],
    fill_color="MediumBlue", alpha=0.5, line_alpha=0, legend_label="Files")
p.ray(x=[np.average(fs)], y=[0], length=0, angle=np.pi/2,
    line_color="MediumBlue", line_width=3)

db_hist, db_edges = np.histogram(db, density=True, bins=bins)
p.quad(top=db_hist, bottom=0, left=db_edges[:-1], right=db_edges[1:],
    fill_color="DarkOrange", alpha=0.5, line_alpha=0, legend_label="SQLite")
p.ray(x=[np.average(db)], y=[0], length=0, angle=np.pi/2,
    line_color="DarkOrange", line_width=3)

p.yaxis.axis_label = "distribution"
p.xaxis.axis_label = "time (ms)"

show(p)
