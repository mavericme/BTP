
import jinja2


templateLoader = jinja2.FileSystemLoader( searchpath="/" )
templateEnv = jinja2.Environment( loader=templateLoader )

TEMPLATE_FILE = "/home/nitin/Desktop/cloud_project_work/final-jinja/10_res.jinja"

template = templateEnv.get_template( TEMPLATE_FILE )

machine = ['10.3.3.20:9610','10.3.3.242:9610']

# Specify any input variables to the template as a dictionary.
templateVars = { "title" : "List Machines_Cluster",
                 "description" : "A simple inquiry of function.",
                 "result" : machine
                  }

outputText = template.render( templateVars )
print outputText


