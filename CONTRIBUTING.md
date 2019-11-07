# Contributing to bloXroute

Thanks for taking the time to contribute to bloXroute and reading this document.

The goal of this document is to capture some general guidelines and be a useful reference for anyone interested in 
contributing to the bloXroute ecosystem. Treat the following content as recommendations, not hard rules. Use your best
judgment and feel free to propose changes – this document will always be a work in progress.

## Conduct

Be nice. You can refer to the [Contributor Covenant] for more detail.

## Communication

Join [our Discord]! You can meet our developer team there to get a sense of what open issues there are that you can 
contribute to and chat broadly about the bloXroute system. Try to avoid filing Github issues just for a question – 
you'll get faster results by talking with us directly in Discord.

## Contributor Workflow

Once you have an issue to work on, you should take the following general steps:

1. [Fork] the repository
2. Create a branch for your changes
3. Write code and create commits
4. Open a pull request against the upstream repo. 

Make sure your pull request passes all builds and add the following users as reviewers:
 * [aspin]
 * [sergeyi23]
 * [shanevc]
 * [Uri-Margalit-bloXroute][urim]
 * [avrahamblx]
 
They will help delegate reviews and merge the PR.

## Style Guide

We primarily adhere to [Google's Python style guide][style], with a couple of addendums:

(2.2) Importing class names is ok. Use packages to reference methods.

```python
# For classes, import directly.
from bxrelay.connections.relay_node import RelayNode
relay = RelayNode()

# For constants, include the module before the call.
import bxcommon.constants
print(constants.CONNECTION_TIMEOUT)

# For module methods, include the module before the method call.
from bxcommon.utils import config
config.init_logging()

# For common libraries, import directly.
from collections import deque
my_double_ended_queue = deque()

# For overloaded module names (e.g. json), reference the module for clarity.
from sanic import response
import json
json.dumps()
response.json({})
```

(2.13) Don’t use `@property`. The `@property` decorator hides a function call, and when doing performance 
optimizations, it's important to know when you're calling a function versus an attribute lookup. Exposing that 
instead of hiding it behind an @property is pretty important. 

(2.18) Be more restrictive here. Don't use `Queue` for multithreading; use locks and condition variables instead. 
Queues have some weird performance characteristics and getting around them requires writing custom C extensions.

(3.2) Maximum line length is 120 characters.

(3.10) Use double quotes `"` for strings

(3.11) For files and sockets, it is important to close them when you're finished with them. 
However, we will rarely if ever use the `with` statement since we likely will be keeping them open for a while. 
It is crucial to close them though.

### Additional Guidelines:

(X.1) Limit one class per file. Multiple internal classes are allowed in the same file.

(X.2) Follow the order for class members:
1. Fields
2. Constructor
3. Class methods
4. Public methods
5. Private methods 

## Builds / Testing

Please make sure your changes include sufficient unit and/or integration test coverage to prove that your code works 
properly. Also, be sure to run the existing unit tests and make sure those pass.

We run builds for static analysis to verify type hints (via [Pyre][pyre]) and linting to check style (via [pylint]).
Make sure these builds pass (you can use `./check.sh` and `./lint.sh` respectively), and include type hints the code
you add.


[Contributor Covenant]: https://www.contributor-covenant.org/version/1/4/code-of-conduct.html
[our Discord]: https://discord.gg/jHgpN8b
[Fork]: https://help.github.com/en/github/getting-started-with-github/fork-a-repo
[aspin]: https://github.com/aspin
[sergeyi23]: https://github.com/sergeyi23
[shanevc]: https://github.com/shanevc
[urim]: https://github.com/Uri-Margalit-bloXroute
[avrahamblx]: https://github.com/avrahamblx
[style]: https://github.com/google/styleguide/blob/gh-pages/pyguide.md
[pyre]: https://pyre-check.org/
[pylint]: https://www.pylint.org/