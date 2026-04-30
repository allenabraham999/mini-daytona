from setuptools import find_packages, setup

setup(
    name="mini-daytona-sdk",
    version="0.1.0",
    description="Python SDK for mini-daytona sandboxes",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(include=["mini_daytona", "mini_daytona.*"]),
    python_requires=">=3.9",
    install_requires=["requests>=2.28"],
)
