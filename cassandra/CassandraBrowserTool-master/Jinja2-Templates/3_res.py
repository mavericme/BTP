
import jinja2


templateLoader = jinja2.FileSystemLoader( searchpath="/" )
templateEnv = jinja2.Environment( loader=templateLoader )

TEMPLATE_FILE = "/home/nitin/Desktop/cloud_project_work/final-jinja/3_res.jinja"

template = templateEnv.get_template( TEMPLATE_FILE )

inputs = ['TK1', 'system', 'testks']	
# Specify any input variables to the template as a dictionary.
templateVars = { "title" : "List Keyspace",
                 "description" : "A simple inquiry of function.",
                 "input" : inputs  }

outputText = template.render( templateVars )
print outputText



