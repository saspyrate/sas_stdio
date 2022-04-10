import logging
import sas_stdio

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

sas = sas_stdio.SASsession(sascmd=["/path/to/sas", "-stdio"])

def submit_verbosely(code):
    print("\n" + 25*"=" + "\n" + 10*">" + " CODE " + 10*">" + "\n" + code + "\n" + 25*"<")
    res = sas.submit(code.encode())
    for x in ["LOG", "LST"]:
        print(10*">" + f" {x} " + 10*">" + res[x].decode() + "\n" + 25*"<" + "\n" + 25*"=")

submit_verbosely(
    "options nosource;"
)
submit_verbosely(
    "%put ERROR: Execution not really terminated by an ABORT CANCEL statement.;\n"
    "%put ### after fake cancelation ###;\n"
    "proc print data = sashelp.cars (obs = 1); run;\n"
    "\n"
    "data _null_; abort cancel; run;\n"
    "%put ### after real cancelation ###;\n"
    "proc print data = sashelp.class (obs = 1); run;"
)
submit_verbosely(
    "%put ### next submit after cancelation ###;\n"
    "proc print data = sashelp.class (obs = 1); run;"
)
submit_verbosely(
    "data _null_; abort return; run;"
)
submit_verbosely(
    "%put ### next submit after return ###;"
)
