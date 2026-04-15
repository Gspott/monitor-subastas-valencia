on run
	set scriptPath to "__PROJECT_ROOT__/scripts/open_monitor_app.command"
	do shell script "/bin/zsh " & quoted form of scriptPath
end run
