
import jinja2


templateLoader = jinja2.FileSystemLoader( searchpath="/" )
templateEnv = jinja2.Environment( loader=templateLoader )

TEMPLATE_FILE = "/home/nitin/Desktop/cloud_project_work/final-jinja/6.jinja"

template = templateEnv.get_template( TEMPLATE_FILE )

inputs = { "ks_name":"Keyspace Name" }

# Specify any input variables to the template as a dictionary.
templateVars = { "title" : "List Column-Family",
                 "description" : "A simple inquiry of function.",
                 "input" : inputs  }

outputText = template.render( templateVars )
print outputText


