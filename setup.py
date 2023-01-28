# noqa: D100
from setuptools import find_packages, setup

__version__ = None
with open("dinao/__version__.py", "r") as fh:
    exec(fh.readlines()[1])
assert __version__, "setup.py: Failed to extract version from dinao/__version__.py"

install_requires = []
tests_require = []

with open("requirements.txt", "r") as fh:
    for line in fh.read().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        elif line.endswith("# test"):
            tests_require.append(line)
        else:
            install_requires.append(line)

with open("README.rst", "r") as fh:
    long_description = fh.read()

setup(
    name="dinao",
    version=__version__,
    packages=find_packages(),
    author="Jim Carreer",
    author_email="jim.carreer+dinao@gmail.com",
    url="https://github.com/jimcarreer/dinao",
    description="A simple database API",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    license="ISC License",
    test_suite="tests",
    install_requires=install_requires,
    tests_require=tests_require,
    python_requires=">=3.8.0, <3.12",
    extras_require={"tests": tests_require},
    classifiers=[
        "Topic :: Database",
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
