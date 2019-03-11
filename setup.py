import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="zoltpy",
    version="0.0.1dev",
    author="Katie House",
    author_email="katiehouse3@gmail.com",
    description="A package of Reich Lab Zoltar utility functions.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/reichlab/zoltpy",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)