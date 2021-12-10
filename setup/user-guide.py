# Attribution: https://blog.danslimmon.com/2019/07/15/do-nothing-scripting-the-key-to-gradual-automation/
# A script to walk through the various steps

import sys

def wait_for_enter():
    input("\033[1;32;40mPress Enter to continue.\033[0;0m")
    print("\n")

def yellow_enter():
    input("\033[1;33;40mPress Enter to continue.\033[0;0m")
    print("\n")

class PrelimSetup(object):
    def run(self, context):
        print(f"If a new machine, login to the CLI with 'az login'")
        print(f"""If a new machine with the 'databricks' extension may not be installed,
                run the command 'az databricks workspace list' to see if that installs
                the extension.""")
        wait_for_enter()

class CreateGithubPAT(object):
    def run(self, context):
        print(f"Create the Github personal token under Developer Settings in Github. Provide 'repo' access.")
        context["github_token"] = input("Input the Github token: ")
        wait_for_enter()

class SetupFiles(object):
    def run(self, context):
        print(f"Ensure there is a 'sub.env' file with a single line: SUB_ID=<your subscription id>")
        print(f"This file should be in the ./setup folder")
        yellow_enter()
        print(f"Run the db_create.sh script. This will setup the workspace.")
        print(f"""If the first time running the Databricks CLI commands, the extension may need to be installed. For more details refer:
                https://docs.microsoft.com/en-us/cli/azure/databricks?view=azure-cli-latest.""")
        #print(f"This will also configure the database CLI. Note the .databrickscfg created at $HOME.")
        wait_for_enter()
 
class DatabricksSettings(object):
    def run(self, context):
        print(f"""Under Admin Console->Workspace Settings, ensure 'DBFS File Browser' is set to 'Enabled'.
                Refresh the browser.""")
        yellow_enter()
        print(f"""Under Admin Console->Workspace Settings, ensure 'Files in Repos' is set to 'Enabled'.
                Refresh the browser.""")
        yellow_enter()
        print(f"""Under User Settings, generate a Personal Access Token (PAT).
            Use this to update the .netrc file in $HOME. Format is:
            machine <instance id> //without https
            login token
            password <Personal Access Token>
                """)
        yellow_enter()
        print(f"Under User Settings->Git integration, add Github personal token.")
        print(f"Provide your Github username")
        print(f"The Github token is: {context['github_token']}")
        wait_for_enter()
 
class DatabricksRepoSettings(object):
    url = 'https://github.com/thomassantosh/exploring-crime-data'
    def run(self, context):
        print(f"""Under Repos, click 'Add Repo' and clone the following URL: {self.url}""")
        wait_for_enter()

class DatabricksCluster(object):
    cluster_name = 'db-cluster'
    cluster_mode = 'Single Node'
    runtime_version = '10.1 (Scala 2.12, Spark 3.2.0)'
    terminate_after = '60 min'
    node_type = 'Standard_DS3_v2'
    def run(self, context):
        print(f"""Run the ./create_cluster.sh script to automatically provision a cluster
        -----OR----------
        Manually create a cluster in the workspace with the following specs:
        cluster_name: {self.cluster_name}, cluster_mode: {self.cluster_mode}, runtime_version: {self.runtime_version}, 
        terminate_after: {self.terminate_after}, node_type: {self.node_type}.""")
        yellow_enter()
        print(f"""After the cluster is up, in the Libraries install two libraries: 'kaleido' and 'plotly'""")
        wait_for_enter()

class RunNotebooks(object):
    def run(self, context):
        print(f"""
        Manually run the 'preprocessing' notebook. This notebook will clean the data, make assumptions about data
        gaps, and attributes, etc. This will also produce a processed CSV in FileStore.""")
        yellow_enter()
        print(f"""
        Manually run the 'normalization' notebook. This notebook has data processing and images for how the trends look
        like normalized for the population, how the top state rankings compare (normalized vs. raw) and how states
        rank against each other when normalized for their z-scores.
                """)
        yellow_enter()
        print(f"""
        Manually run the 'correlations' notebook. This will produce a correlation matrix for each trend line (state and
        crime combination) and produce a correlation matrix output in FileStore. (Note that this is not the
        complete correlation matrix, but only for the first 10 columns.)""")
        wait_for_enter()

class DownloadFiles(object):
    def run(self, context):
        print(f"""Run the get_files.sh script in the datasets folder. This downloads the encoded files stored
                at the FileStore through the REST API with the file paths specified in the json files.""")
        yellow_enter()
        print(f"""Run the following two commands to decode the files:
            python decoder.py './encoded_processed.txt' 'processed.csv'
            python decoder.py './encoded_correlation.txt' 'correlation.csv'
                """)
        wait_for_enter()


if __name__ == "__main__":
    context = {}
    procedure = [
            PrelimSetup(),
            CreateGithubPAT(),
            SetupFiles(),
            DatabricksSettings(),
            DatabricksRepoSettings(),
            DatabricksCluster(),
            RunNotebooks(),
            DownloadFiles(),
    ]
    for i, step in enumerate(procedure):
        print(f'Step {i}: ')
        step.run(context)
    print("Done. All steps complete.")
