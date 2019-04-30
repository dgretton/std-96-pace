import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import pdb
import sys
import os

number_of_turb = 96
db_dir = os.path.join('..', 'method_local')

if len(sys.argv) > 2:
    print('Only (optional) argument is the name of the database you want to plot from')
    exit()
dbs = [filename for filename in os.listdir(db_dir) if filename.split('.')[-1] == 'db']
if len(sys.argv) == 2:
    db_name = sys.argv[1]
    if db_name not in dbs:
        print('database does not exist in ' + db_dir)
        exit()
else:
    if len(dbs) != 1:
        print('can\'t infer which database you want to plot from, please specify with argument')
        exit()
    db_name, = dbs

conn = sqlite3.connect(os.path.join(db_dir, db_name))
c = conn.cursor()

for type in ['lum', 'abs']:
    scale = 2
    fig1 = plt.figure(figsize=(24*scale, 16*scale))
    subplot = 0
    for column in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
        for row in range(1,13):
            subplot = subplot + 1
            well = column + str(row)
            
            # set up plot
            ax = fig1.add_subplot(8, number_of_turb/8, subplot)
            ax.set_title("Lagoon" + well, x=0.5, y=0.8)

            n = (well, type, )
            c.execute('SELECT filename, well, reading FROM measurements WHERE well=? AND data_type=?', n)
   
            x = c.fetchall()
            vals = [(datetime.strptime(f[-15:-4], '%y%m%d_%H%M'), w, v) for (f, w, v) in x]
            #pdb.set_trace()
            vals = [(t, w, v) for t, w, v in vals if t > datetime(2018, 11, 1, 18, 15)] 
    
            #assert w == well, "Recorded lagoon " + str(w) + " not equal to expected lagoon well " + str(well)

            plt.plot([j for (j, _, _) in vals], [lum for (j, _, lum) in vals], 'b.-')
    
            if type == 'abs':
                plt.ylim(0.0, 1.0)
            else:
                plt.ylim(300, 2000)
        
            # decrease number of plotted X axis labels
            # make there be fewer labels so that you can read them
            times = [x for (x, _, _) in vals]
            deltas = [t - times[0] for t in times]
            labels = [int(d.seconds/60/60 + d.days*24) for d in deltas]
            labels_sparse = [labels[x] if x % 6 == 0 else '' for x in range(len(labels))]
            plt.xticks(times, labels_sparse)
            locs, labels = plt.xticks()
        print(len(x), "entries fetched")
    
    fig1.tight_layout()
    plt.savefig(os.path.join(db_dir, 'plot_' + type + ".png"), dpi = 200)

conn.close()