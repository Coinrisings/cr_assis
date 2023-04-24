/opt/anacaonda3/bin/python3 "/home/ssh/jupyter/cr_assis/cr_assis/run/run_update.py"
/opt/anacaonda3/bin/python3 "/home/ssh/jupyter/cr_monitor/cr_monitor/run/run_monitor.py"
cd /home/ssh/parameters
/usr/bin/git pull
/usr/bin/git commit -a -m "daily update contractsize"
/usr/bin/git push