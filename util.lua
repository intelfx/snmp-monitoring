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

	function util.caller()
		return dbg_info[#dbg_info - 2] or "(top level)"
	end

	function util.dbg(fmt, ...)
		return printf("DBG [%-20s]: " .. fmt, util.caller(), ...)
	end
else
	function util.caller()
		return "(not available)"
	end

	function util.dbg(fmt, ...)
	end
end

dbg = util.dbg

function util.assert_eq(arg1, arg2)
	assert(arg1 == arg2, string.format("assertion failed: '%s' ~= '%s'", tostring(arg1), tostring(arg2)))
end

assert_eq = util.assert_eq

function util.is_string(arg)
	return arg and type(arg) == "string"
end

function util.is_table(arg, n)
	return arg and type(arg) == "table" and (not n or #arg == n)
end

function util.is_function(arg)
	return arg and type(arg) == "function"
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

function util.last(arg)
	return arg[#arg]
end

function util.set_last(arg, value)
	arg[#arg] = value
	return value
end

local function __util_dir_iter(state)
	-- returned from last directory?
	if #state.stack < 1 then
		return nil
	end

	if #state.stack == #state.pieces + 1 then
		-- entered new directory? create missing placeholder for the last path component
		table.insert(state.pieces, {})
	else
		-- clear the placeholder for the last path component, including any user data
		assert(#state.stack == #state.pieces)
		util.set_last(state.pieces, {})
	end

	-- shortcuts
	local stack_top = util.last(state.stack)
	local pieces_top = util.last(state.pieces)

	-- get next directory entry, skip . and ..
	local key
	repeat
		key = stack_top.iter(stack_top.state)
	until key ~= "." and key ~= ".."

	-- no next directory entry? return from current directory
	if not key then
		table.remove(state.stack)
		table.remove(state.pieces)
		return __util_dir_iter(state)
	end

	local subpath = util.join(stack_top.subpath, key)
	local subpath_abs = util.join(state.root, subpath)
	local mode = lfs.attributes(subpath_abs, "mode")

	-- fill in the last component of the subpath
	pieces_top.key = key
	pieces_top.subpath = subpath

	if mode == "directory" then
		-- descend (to never recover the pieces)
		stack_top = { subpath = subpath }
		stack_top.iter, stack_top.state = lfs.dir(subpath_abs)
		table.insert(state.stack, stack_top)

		if state.report_dirs then
			-- return the directory itself, the subsequent call will continue into it
			return subpath, mode, state
		else
			-- tailcall to return the first file in the directory
			return __util_dir_iter(state)
		end
	else
		-- return the read entry
		return subpath, mode, state
	end

end

function util.dir(path, report_dirs)
	local state = { root = path, report_dirs = report_dirs and true or false, stack = { { subpath = "" } }, pieces = { } }

	state.stack[1].iter, state.stack[1].state = lfs.dir(path) -- inner iterator
	return __util_dir_iter, state
end

function util.dir_prune(state)
	table.remove(state.stack)
	table.remove(state.pieces)
end

function util.assign_assert(tbl, index, value)
	if tbl[index] then
		assert_eq(tbl[index], value)
	else
		tbl[index] = value
	end
end

function util.accumulate_max(tbl, index, value)
	if not tbl[index] or value > tbl[index] then
		tbl[index] = value
	end
end

function util.table_join(lower, upper)
	for k, v in pairs(upper) do
		if util.is_table(v) and util.is_table(lower[k]) then
			util.table_join(lower[k], v)
		else
			lower[k] = v
		end
	end

	return lower
end

function util.table_join_assert(lower, upper)
	for k, v in pairs(upper) do
		if util.is_table(v) and util.is_table(lower[k]) then
			util.table_join_assert(lower[k], v)
		else
			util.assign_assert(lower, k, v)
		end
	end

	return lower
end

function util.in_set(arg, tbl)
	for i, v in ipairs(tbl) do
		if arg == v then
			return i
		end
	end
	return nil
end

function util.table_map(input, op)
	local result = { }

	if util.is_function(op) then
		for k, v in pairs(input) do
			result[k] = op(v)
		end
	else -- assume op to be field name to extract
		for k, v in pairs(input) do
			assert(util.is_table(v), "We're told to extract field '%s' from each entry of the table, but entry with key '%s' is a %s", tostring(op), tostring(k), type(v))
			result[k] = v[op]
		end
	end

	return result
end

function util.new(proto, object)
	if not object then
		object = { }
	end

	setmetatable(object, { __index = proto })

	return object
end

function util.tonumber(arg)
	if arg then
		return assert(tonumber(arg))
	else
		return nil
	end
end

return util
