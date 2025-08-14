#!/bin/sh

#export PROC_PID=1

export PROC_NAME_LIKE='%bash%'
export PROC_NAME_LIKE='%zsh%'
export PROC_NAME_LIKE='%screen%'

export ENV_SQL='
	SELECT
		*
	FROM proc_table
	ORDER BY
		memory DESC,
		pid    ASC
'

\time -l ./procs2df
