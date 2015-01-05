
import jinja2


templateLoader = jinja2.FileSystemLoader( searchpath="/" )
templateEnv = jinja2.Environment( loader=templateLoader )

TEMPLATE_FILE = "/home/prajit/cloud_final/jinja/project/main.jinja"

template = templateEnv.get_template( TEMPLATE_FILE )

FAVORITES = [ "Create Keyspace", "Drop Keyspace", "List Keyspace", "Create Column-Family", "Drop Column-Family", "List Column-Family", "Add Entry to Column-Family", "Drop Entry from Column-Family", "Display Entry from Column-Family", "List Machines_Cluster" ]


# Specify any input variables to the template as a dictionary.
templateVars = { "title" : "Cassandra Browser Tool",
                 "description" : "A simple inquiry of function.",
                 "favorites" : FAVORITES
               }
               
outputText = template.render( templateVars )
print outputText



