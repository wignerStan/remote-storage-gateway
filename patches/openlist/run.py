#!/usr/bin/env python3
"""Verify the package with the installed Git skill; acquisition belongs to Go derive."""
from pathlib import Path
import runpy, sys
skill=Path.home()/".agents/skills/git/scripts/check_git_contract.py"
if not skill.is_file():
    raise SystemExit("Install the Git skill before package verification")
sys.argv=[str(skill),"relationship", "--repo",str(Path(__file__).resolve().parents[2]),"--contract",str(Path(__file__).resolve().parents[2]/".git-source-relationships/openlist.json")]
runpy.run_path(str(skill),run_name="__main__")
