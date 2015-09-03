-- db ops

-- global db_root: string
--                 specifies the global database root

-- arg ctx: table { path: string, entries: table { key=value ... } }
--          describes a certain point (path) in database
--          @path is physical path relative to @db_root
--          @entries holds all key=value pairs encountered during traversal to this point, including file attributes at this point

local util = require("util")
local db = {}

local __db_check = function(ctx)
	assert(util.is_table(ctx))
	assert(util.is_string(ctx.root))
	assert(util.is_string(ctx.path))
	assert(util.is_table(ctx.entries))

	for k, v in pairs(ctx.entries) do
		assert(util.is_string(k))
		assert(util.is_string(v))
	end

	if ctx.path ~= "" then
		assert(lfs.attributes(util.join(ctx.root, ctx.path), "mode") == "directory")
	end
	return ctx
end

local __db_read = function(ctx)
	__db_check(ctx)

	local path_abs = ctx:path_full()
	for key in lfs.dir(path_abs) do
		local subpath_abs = util.join(path_abs, key)
		if lfs.attributes(subpath_abs, "mode") == "file" then
			assert (not ctx.entries[key])
			ctx.entries[key] = util.read_all(subpath_abs)
		end
	end

	return ctx
end

function db.new(root)
	assert(util.is_string(root), "database root is not a string")

	local obj = { root = root, path = "", entries = {} }
	setmetatable(obj, { __index = db })
	return __db_check(obj)
end

function db.root_with(ctx, ...)
	__db_check(ctx)

	return util.join(ctx.root, ...)
end

function db.path_full(ctx)
	__db_check(ctx)

	return util.join(ctx.root, ctx.path)
end

function db.path_with(ctx, ...)
	__db_check(ctx)

	return util.join(ctx.path, ...)
end

function db.path_full_with(ctx, ...)
	__db_check(ctx)

	return util.join(ctx.root, ctx.path, ...)
end

-- arg purge: string
--            whether to remove all attributes and nested entries at descend destination

-- arg ...: table { { key, value } ... }
--          is an array of { key, value } arrays describing entries to descend to

function db.descend(ctx, purge, ...)
	__db_check(ctx)

	local n = select("#", ...)

	for i, entry in ipairs { ... } do
		assert(util.is_table(entry, 2))

		local key, value = entry[1], entry[2]
		assert(util.is_string(key))
		assert(util.is_string(value))

		local new_path = ctx:path_with(key .. "=" .. value)
		local new_path_abs = ctx:root_with(new_path)

		local new_path_mode = lfs.attributes(new_path_abs, "mode")
		if new_path_mode then
			assert(new_path_mode == "directory")
		else
			lfs.mkdir(new_path_abs)
		end

		ctx.path = new_path
		ctx.entries[key] = value

		if i == n and purge then
			util.rmdir(new_path_abs, true)
		else
			__db_read(ctx)
		end
	end

	return ctx
end

-- arg ...: table { { key, value } ... }
--          is an array of { key, value } arrays describing entries to descend to

function db.can_descend(ctx, ...)
	__db_check(ctx)

	local path_abs = ctx:path_full(), path_mode

	for i, entry in ipairs { ... } do
		assert(util.is_table(entry, 2))

		local key, value = entry[1], entry[2]
		assert(util.is_string(key))
		assert(util.is_string(value))

		path_abs = util.join(path_abs, key .. "=" .. value)
		if lfs.attributes(path_abs, "mode") ~= "directory" then
			return false
		end
	end

	return true
end

function db.set(ctx, key, value)
	__db_check(ctx)
	assert(not ctx.entries[key])

	local path_abs = ctx:path_full_with(key)
	assert(not lfs.attributes(path_abs, "mode"))

	local file = io.open(path_abs, "w")
	file:write(value)
	file:close()

	ctx.entries[key] = value
end

function db.get(ctx, key)
	__db_check(ctx)

	return ctx.entries[key]
end

return db
