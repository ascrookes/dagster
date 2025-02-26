from setuptools import find_packages, setup


def get_version():
    version = {}
    with open("dagster_msteams/version.py") as fp:
        exec(fp.read(), version)  # pylint: disable=W0122

    return version["__version__"]


if __name__ == "__main__":
    setup(
        name="dagster-msteams",
        version=get_version(),
        author="Elementl",
        author_email="hello@elementl.com",
        license="Apache-2.0",
        description="A Microsoft Teams client resource for posting to Microsoft Teams",
        url="https://github.com/dagster-io/dagster/tree/master/python_modules/libraries/dagster-msteams",
        classifiers=[
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "License :: OSI Approved :: Apache Software License",
            "Operating System :: OS Independent",
        ],
        packages=find_packages(exclude=["dagster_msteams_tests*"]),
        install_requires=[
            "dagster",
            "requests>=2,<3",
        ],
        zip_safe=False,
    )
