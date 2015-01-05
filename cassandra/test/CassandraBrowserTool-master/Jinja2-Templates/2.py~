
import jinja2


templateLoader = jinja2.FileSystemLoader( searchpath="/" )
templateEnv = jinja2.Environment( loader=templateLoader )

TEMPLATE_FILE = "/home/nitin/Desktop/cloud_project_work/final-jinja/2.jinja"

inputs = {  "ks_name":"Keyspace Name" }

template = templateEnv.get_template( TEMPLATE_FILE )


# Specify any input variables to the template as a dictionary.
templateVars = { "title" : "Drop Keyspace",
                 "description" : "A simple inquiry of function.",
                 "input" : inputs  }

outputText = template.render( templateVars )
print outputText










