local util = {}

function util.printf(fmt, ...)
	print(string.format(fmt, ...))
end

printf = util.printf

if os.getenv("FRAMEWORK_DEBUG") then
	local dbg_info = {}

	debug.sethook(function(event)
		if event == "call" then
			local info = debug.getinfo(2)

			if not info then
				table.insert(dbg_info, "<unknown>")
			elseif info.what ~= "Lua" then
				table.insert(dbg_info, "<C function>")
			else
				table.insert(dbg_info, info.name)
			end
		elseif event == "return" or event == "tail return" then
			table.remove(dbg_info)
		end
	end, "cr")

	function util.dbg(fmt, ...)
		return printf("DBG [%s]: " .. fmt, dbg_info[#dbg_info], ...)
	end
else
	function util.dbg(fmt, ...)
	end
end

dbg = util.dbg

function util.is_string(arg)
	return arg and type(arg) == "string"
end

function util.is_table(arg, n)
	return arg and type(arg) == "table" and (not n or #arg == n)
end

function util.clone(t) -- deep-copy a table
	if type(t) ~= "table" then return t end
	local meta = getmetatable(t)
	local target = {}
	for k, v in pairs(t) do
		target[k] = (type(v) == "table") and util.clone(v) or v
--		if type(v) == "table" then
--			target[k] = clone(v)
--		else
--			target[k] = v
--		end
	end
	setmetatable(target, meta)
	return target
end

function util.join(...)
	local args = { ... }
	local path = ""
	for i, piece in ipairs(args) do
		if path == "" then
			path = piece
		else
			path = path .. "/" .. piece
		end
	end
	return path
end

function util.rmdir(path, no_remove_final)
	for file in lfs.dir(path) do
		if file ~= "." and file ~= ".." then
			local subpath = util.join(path, file)
			if lfs.attributes(subpath, "mode") == "directory" then
				util.rmdir(subpath)
			else
				os.remove(subpath)
			end
		end
	end

	if not no_remove_final then
		os.remove(path)
	end
end

function util.read_all(path)
	local file = io.open(path, "r")
	local data = file:read("*a")
	file:close()
	return data
end

return util
