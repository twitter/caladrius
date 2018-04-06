import hug

import caladrius.api.root as root

@hug.extend_api("")
def root_api():
    return [root]

if __name__ == "__main__":

    print("Running Caladrius!")

    hug.API(__name__).http.serve()
