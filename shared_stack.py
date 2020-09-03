#!/usr/bin/env python
#
# LSST Data Management System
#
# Copyright 2008-2016  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
"""
Shared-stack maintenance.

This tool builds and maintains a "shared stack" installation of the LSST
Science Pipelines. It is designed to run on the LSST developer infrastructure
(``lsst-dev``, etc); it may be useful elsewhere.

End users looking to install the stack for personal use should prefer the
procedure described on the `DM Website <http://dm.lsst.org>`_. Developers
looking to build the latest versions of the LSST code should prefer the `build
tool <http://developer.lsst.io/en/latest/build-ci/lsstsw.html>`_.

Specifically, when pointed (``ROOT``, defined below) at a
directory which does not exist, we:

- Bootstrap a new stack installation in that directory, using the standard
  LSST newinstall.sh script;
- Add some optional Conda packages that developers find convenient;
- Set the EUPS configuration to disable locking (usually a good idea).

When ``ROOT`` exists and contains an installed version of EUPS, the following
maintainance procedure is followed:

- Contact the EUPS distribution server (``EUPS_PKGROOT``, below), retrieving a
  the contents of all tags that match the ``VERSION_GLOB`` expression.
- For all specified products (``PRODUCTS``, below), identify tags retrieved
  from the server which have not been installed and install them.
- Sort the installed tags by date they were created on the server and tag the
  most recent as "current".

This tool requires Python (tested with 2.6, 2.7 and 3.6) and `lxml
<http://lxml.de/>`_; the latter may be conveniently installed using ``pip``::

  $ pip install -r requirements.txt

With the exception of the target directory, which can be over-ridden on the
command line, all configuration is performed by editing the ``CONFIGURATION``
block below.
"""
from __future__ import print_function

import os
import shutil
import re
import subprocess
import tempfile
from argparse import ArgumentParser
from datetime import datetime
from lxml import html
try:
    # Python 3
    from urllib.request import urlopen
except ImportError:
    # Python 2
    from urllib2 import urlopen

#
# CONFIGURATION
#

# Set to True to output detailed information on commands being executed and
# their environment.
DEBUG = False

# Package distribution server to use.
EUPS_PKGROOT = "https://eups.lsst.codes/stack/src"

# newinstall.sh location
NEWINSTALL_URL = "https://raw.githubusercontent.com/lsst/lsst/master/scripts/newinstall.sh"

# Python version to be requested from newinstall.sh
PYVER = "3"

# Tuples of (name, version) to be installed using Conda before we add the
# stack. Version of `None` is equivalent to "don't care".
CONDA_PKGS = [
    ("jupyter", None),
    ("pep8", None),
    ("pyflakes", None),
    ("panel", None),
    ("holoviews", None),
    ("hvplot", None),
    ("bokeh", None),
    ("pyviz_comms", None),
    ("fastparquet", None),
    ("numba", None),
    ("datashaderpyct", None),
    ("dask-jobqueue", None),
    ("cx_Oracle", None),
    ("ipdb", None),
    ("psycopg2", None),
]

# Top-level products to install into the stack.
PRODUCTS = ["lsst_sims", "lsst_distrib"]

# Root directory in which the stack will be created or updated.
ROOT = '/ssd/lsstsw/stack'

# VERSION_GLOB is now set by the argparser.

# Create a Conda environment with this name for the shared stack installation.
# Note that a Conda environment with some sort of name is required by
# newinstall.sh; here, we choose something easy to predict, rather than trying
# to determine it from a SHA1.
LSST_CONDA_ENV_NAME="lsst-scipipe"

def determine_flavor():
    """
    Return a string representing the 'flavor' of the local system.

    Based on the equivalent logic in EUPS, but without introducing an EUPS
    dependency.
    """
    uname, machine = os.uname()[0:5:4]
    if uname == "Linux":
        if machine[-2:] == "64":
            return "Linux64"
        else:
            return "Linux"
    elif uname == "Darwin":
        if machine in ("x86_64", "i686"):
            return "DarwinX86"
        else:
            return "Darwin"
    else:
        raise RuntimeError("Unknown flavor: (%s, %s)" % (uname, machine))


class Product(object):
    """
    Information about a particular EUPS product.

    This includes the the product name, the available versions and their
    associated tags (if any).
    """
    def __init__(self, name):
        self.name = name

        # Map from version to tags corresponding to that version.
        # NB cannot use a default dict, because we need to distinguish between
        # versions which have no tags and versions which do not exist.
        self._versions = {}

    def add_version(self, version):
        if version not in self._versions:
            self._versions[version] = set()

    def add_tag(self, version, tag):
        self._versions[version].add(tag)

    def versions(self, tag=None):
        """
        Return a list of versions of the product. If ``tag`` is not ``None``,
        return only those versions tagged ``tag``.
        """
        if tag is None:
            return self._versions.keys()
        else:
            return [k for k, v in self._versions.items() if tag in v]

    def tags(self, version=None):
        """
        Return a list of tags applied to the product. If ``version`` is not
        ``None``, return only those tags which refer to ``version``.
        """
        if version is None:
            return set.union(*self._versions.values())
        else:
            return self._versions[version]


class ProductTracker(object):
    """
    Track a collection of Products.
    """
    def __init__(self):
        self._products = {}

    def tags_for_product(self, product_name):
        """
        Return the set of all tags which contain a product
        named ``product_name``.
        """
        try:
            return self._products[product_name].tags()
        except KeyError:
            return set()

    def products_for_tag(self, tag):
        """
        Return a list of (product_name, version) tuples which are tagged with
        ``tag``.
        """
        results = []
        for product in self._products.values():
            versions = product.versions(tag=tag)
            for version in versions:
                results.append((product.name, version))
        return results

    def current(self, product_name):
        """
        Return the version of product_name which is tagged "current", or None.
        """
        if product_name in self._products:
            return self._products[product_name].versions("current")[0]

    def has_version(self, product_name, version):
        """
        Return True if we have the given version of product name.
        """
        return (product_name in self._products and
                version in self._products[product_name].versions())

    def insert(self, product, version, tag=None):
        """
        Add (product, version, tag) to the list of products being tracked.
        """
        if product not in self._products:
            self._products[product] = Product(product)
        self._products[product].add_version(version)
        if tag:
            self._products[product].add_tag(version, tag)


class RepositoryManager(object):
    """
    Provide access to a ProductTracker built on a remote repository.
    """
    def __init__(self, pkgroot=EUPS_PKGROOT, pattern=r".*"):
        """
        Only tags which match regular expression ``pattern`` are recorded.
        More tags -> slower loading.
        """
        self._product_tracker = ProductTracker()
        self.tag_dates = {}
        self.pkgroot = pkgroot

        h = html.parse(urlopen(self.pkgroot + "/tags"))
        for el in h.findall("./body/table/tr/td/a"):
            if el.text[-5:] == ".list" and re.match(pattern, el.text):
                u = urlopen(pkgroot + '/tags/' + el.get('href'))
                tag_date = datetime.strptime(u.info()['last-modified'],
                                             "%a, %d %b %Y %H:%M:%S %Z")
                self.tag_dates[el.text[:-5]] = tag_date
                for line in u.read().decode('utf-8').strip().split('\n'):
                    if ("EUPS distribution %s version list" %
                       (el.text[:-5]) in line):
                        continue
                    if line.strip()[0] == "#":
                        continue
                    product, flavor, version = line.split()
                    self._product_tracker.insert(product, version,
                                                 el.text[:-5])

    def tags_for_product(self, product_name):
        return self._product_tracker.tags_for_product(product_name)

    def products_for_tag(self, tag):
        return self._product_tracker.products_for_tag(tag)


class StackManager(object):
    """
    Tools for working with an EUPS product stack.

    Includes the functionality of a ProductTracker together with routines for
    creating and manipulating the stack.
    """
    def __init__(self, stack_dir, pkgroot=EUPS_PKGROOT,
                 userdata=None, debug=DEBUG):
        """
        Create a StackManager to manage the stack in ``stack_dir``.

        ``stack_dir`` should already exist and contain an EUPS installation
        (see StackManager.create_stack() if it doesn't).

        Use the remote ``pkgroot`` as a distribution server when installing
        new products.

        Store user data (e.g. the EUPS cache) in ``userdata``, rather than the
        current user's home directory, if supplied. This means that multiple
        StackManagers can be operated by the same user simultaneously without
        conflict.

        Write verbose debugging information if ``debug`` is ``True``.
        """
        self.stack_dir = stack_dir
        self.flavor = determine_flavor()

        # Generate extra output
        self.debug = debug

        # Construct the environment for running EUPS by sourcing loadLSST.bash
        # and replicating what it does to the environment.
        self.eups_environ = dict(var.split(b"=", 1) for var in
                                 subprocess.check_output("source %s ; env -0" % (os.path.join(stack_dir, "loadLSST.bash"),), shell=True).split(b'\x00')
                                 if len(var.split(b"=", 1)) == 2)
        if userdata:
            self.eups_environ["EUPS_USERDATA"] = userdata

        self._refresh_products()

    def _refresh_products(self):
        """
        Update the list of products we track in this stack.

        Should be run whenever the stack state is changed (e.g. by installing
        new products).
        """
        self._product_tracker = ProductTracker()

        for line in self._run_cmd("list", "--raw").strip().split('\n'):
            if line == '':
                continue
            product, version, tags = line.split("|")
            if tags == '':
                self._product_tracker.insert(product, version)
            for tag in tags.split(":"):
                if tag in ("setup"):
                    continue
                self._product_tracker.insert(product, version, tag)

    def _run_cmd(self, cmd, *args):
        """
        Run an ``eups`` command to manipulate the local stack.
        """
        to_exec = ['eups', '--nolocks', cmd]
        to_exec.extend(args)
        if self.debug:
            print(self.eups_environ)
            print(to_exec)
        return StackManager._check_output(to_exec, env=self.eups_environ,
                                          universal_newlines=True)

    def set_config(self, line):
        """
        Add a line to the stack's startup.py file.
        """
        startup_path = os.path.join(self.stack_dir, "eups", "current",
                                    "site", "startup.py")
        with open(startup_path, "a") as startup_py:
            startup_py.write(line + "\n")

    def conda(self, action, package_name, version=None):
        """
        Perform ``action`` ("install", "remove", etc) on package named
        ``package_name``. If supplied, version is appended to the package name
        (thus ``package_name=version``).

        Returns the output from executing the command.
        """
        if version:
            package = "%s=%s" % (package_name, version)
        else:
            package = package_name
        to_exec = ["conda", action, "--name", LSST_CONDA_ENV_NAME, "--yes", package]
        if "action" == "install":
            to_exec.insert(2, "--no-update-deps")
        if self.debug:
            print(self.eups_environ)
            print(to_exec)
        return StackManager._check_output(to_exec, env=self.eups_environ,
                                          universal_newlines=True)

    def tags_for_product(self, product_name):
        return self._product_tracker.tags_for_product(product_name)

    def version_from_tag(self, product_name, tag):
        """
        Return the version of ``product_name`` which is tagged ``tag``.
        """
        for product, version in self._product_tracker.products_for_tag(tag):
            if product == product_name:
                return version

    def distrib_install(self, product_name, version=None, tag=None):
        """
        Use ``eups distrib`` to install ``product_name``.

        If ``version`` and/or ``tag`` are specified, ask for them explicitly.
        Otherwise, accept the defaults.
        """
        args = ["install", "--no-server-tags", product_name]
        if version:
            args.append(version)
        if tag:
            args.extend(["-t", tag])
        print(self._run_cmd("distrib", *args))
        self._refresh_products()

    def add_global_tag(self, tagname):
        """
        Add a global tag to the stack's startup.py file.

        Note that it is -- with some exceptions -- only possible to tag
        products with tags that have been pre-declared in startup.py.
        Therefore, we need to call this before we can use ``apply_tag()``.
        """
        self.set_config('hooks.config.Eups.globalTags += ["%s"]' %
                        (tagname,))

    def tags(self):
        """
        Return a list of all tags in the stack.
        """
        return self._run_cmd("tags").split()

    def apply_tag(self, product_name, version, tagname):
        """
        Apply ``tagname`` to ``version`` of ``product_name``.

        Note that ``tagname`` must generally have been
        pre-declared using ``add_global_tag()``.
        """
        if self._product_tracker.has_version(product_name, version):
            self._run_cmd("declare", "-t", tagname, product_name, version)
            self._product_tracker.insert(product_name, version, tagname)

    @staticmethod
    def create_stack(stack_dir, pkgroot=EUPS_PKGROOT,
                     userdata=None, debug=DEBUG):
        """
        Bootstrap a stack in ``stack_dir`` by fetching & running newinstall.sh.

        Arguments are as for ``StackManager.__init__()``, but note that
        ``stack_dir`` must not already exist.

        An initialized StackManager is returned.
        """
        # Refuses to proceed if ``stack_dir`` already exists.
        os.makedirs(stack_dir)

        # We'll use newinstall.sh to bootstrap our stack according to current
        # "best" practice.
        newinstall_filename = os.path.join(stack_dir, "newinstall.sh")
        with open(newinstall_filename, "wb") as newinstall_file:
            newinstall_file.write(urlopen(NEWINSTALL_URL).read())

        newinstall_environ = os.environ.copy()
        newinstall_environ.update({"LSST_CONDA_ENV_NAME": LSST_CONDA_ENV_NAME})

        subprocess.check_call(["/bin/bash", newinstall_filename, "-b", "-" + PYVER],
                              env=newinstall_environ, cwd=stack_dir)

        sm = StackManager(stack_dir, pkgroot=pkgroot,
                          userdata=userdata, debug=debug)
        sm.set_config("hooks.config.site.lockDirectoryBase = None")  # DM-8872

        for pkg in CONDA_PKGS:
            sm.conda("install", pkg[0], pkg[1])

        return sm

    @staticmethod
    def _check_output(*popenargs, **kwargs):
        """
        Run an external command, check its exit status, and return its output.
        """
        # This is effectively  subprocess.check_output() function from
        # Python 2.7+ provided here for compatibility with Python 2.6.
        process = subprocess.Popen(stdout=subprocess.PIPE,
                                   *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            print("Failed process output:")
            print(output)
            if cmd is None:
                cmd = popenargs[0]
            raise subprocess.CalledProcessError(retcode, cmd)
        return output


def main(cfg):
    # We create a temporary directory for the EUPS cache etc. This means we
    # can run multiple instances of StackManager simultaneously without them
    # clobbering each other.
    userdata = tempfile.mkdtemp()

    # If the stack doesn't already exist, create it.
    if not os.path.exists(cfg.root):
        sm = StackManager.create_stack(cfg.root, userdata=userdata)
    else:
        sm = StackManager(cfg.root, userdata=userdata)

    rm = RepositoryManager(pattern=cfg.version_glob)

    for product in cfg.products:
        print("Considering %s" % (product,))
        server_tags = rm.tags_for_product(product)
        installed_tags = sm.tags_for_product(product)
        candidate_tags = server_tags - installed_tags

        for tag in candidate_tags:
            print("  Installing %s tagged %s" % (product, tag))
            try:
                sm.distrib_install(product, tag=tag)
                if tag not in sm.tags():
                    print("  Adding global tag %s" % (tag,))
                    sm.add_global_tag(tag)

                print("  Applying tag %s" % (tag,))
                for sub_product, version in rm.products_for_tag(tag):
                    sm.apply_tag(sub_product, version, tag)

            except subprocess.CalledProcessError:
                print("  Failed to install %s tagged %s; skipping" % (product, tag))

        # Tag as current based on date ordering on server.
        available_tags = server_tags.intersection(sm.tags_for_product(product))
        if available_tags:  # Could be an empty set
            current_tag = max(available_tags,
                              key=lambda tag: rm.tag_dates[tag])
            print("  Marking %s %s as current" % (product, current_tag))
            for sub_product, version in rm.products_for_tag(current_tag):
                sm.apply_tag(sub_product, version, "current")

    shutil.rmtree(userdata)


if __name__ == "__main__":
    parser = ArgumentParser(description="Maintain a shared EUPS stack.")
    parser.add_argument('--root', help="target directory", default=ROOT)
    parser.add_argument('--products', nargs="+", help="products to install", default=PRODUCTS)
    # Only tags matching this regular expression will be fetched from
    # ``EUPS_PKGROOT`` and hence considered for local installation. The more tags
    # are matched, the slower things will be.
    #VERSION_GLOB = r"(sims_)?w_2019_\d\d|v17_0|v17_0_1"
    #VERSION_GLOB = r"(sims_)?w_2019_(1[2-9]|[2-5]\d)"
    #VERSION_GLOB = r"((sims_)?w_2019_(1[2-9]|[2-5]\d))|(d_2019_09_30)"
    #VERSION_GLOB = r"(sims_)?w_2019_(4[3-9]|5\d)"
    #VERSION_GLOB = r"w_2020_(0[7-9]|[1-5]\d)"
    #VERSION_GLOB = r"(sims_)?w_2020_[1-5]\d"
    VERSION_GLOB = r"(sims_)?w_2020_(18|19|[2-5]\d)"
    parser.add_argument('--version-glob', help="pattern to install", default=VERSION_GLOB)
    main(parser.parse_args())
