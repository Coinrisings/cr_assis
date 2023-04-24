/opt/anacaonda3/bin/python3 "/home/ssh/jupyter/cr_assis/cr_assis/run/run_buffet2.py"
/opt/anacaonda3/bin/python3 "/home/ssh/jupyter/service-online/buffet2.0/run_buffet2.0.py"
cd /home/ssh/parameters
/usr/bin/git pull
/usr/bin/git commit -a -m "daily update contractsize"
/usr/bin/git push