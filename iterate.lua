dkjson = require("dkjson")
lfs = require("lfs")
http = require("socket.http")

util = require("util")
db = require("db")
log = require("log")

ctx = db.new("db")

-- this database is internally used by the renderer
state_ctx = db.new("renderer-state")
state_cache = { } -- per-device-id state context cache

-- this is a form of intermediate representation between the on-disk form structured for easy writing and human introspection
-- and the Plot.ly JSON representation. It is intended to be "structurally almost identical" to the latter.
ir_buffer = { }

-- state/config ops

function get_device_id(ctx)
	return string.format("%s#%s", ctx:get("device"), ctx:get("marker"))
end

function state_create_device(device_id)
	if state_cache[device_id] then
		return state_cache[device_id]
	else
		local state = { }
		state.ctx = util.clone(state_ctx):descend(os.getenv("RECREATE") and true or false, { "device", device_id })
		state.last_timestamp = util.tonumber(state.ctx:get("last-timestamp"))
		state.config = dkjson.decode(state.ctx:get("config") or "{}")
		state_cache[device_id] = state
		return state
	end
end

function state_reset_device(device_id)
	state_cache[device_id] = nil
end

function state_save_all()
	for device_id, state in pairs(state_cache) do
		if state.last_timestamp_uncommitted then
			state.ctx:set("last-timestamp", tostring(state.last_timestamp_uncommitted))
		end
		state.ctx:set("config", dkjson.encode(state.config, { indent = true }))
	end
end

function ir_measurement_is_recent(ctx)
	local state = state_create_device(get_device_id(ctx))
	local measurement_timestamp = util.tonumber(ctx:get("timestamp"))

	if not state.last_timestamp or measurement_timestamp > state.last_timestamp then
		state.last_timestamp_uncommitted = measurement_timestamp
		dbg("Measurement at subpath '%s' is recent (%d > %d)", ctx.path, measurement_timestamp, state.last_timestamp or -1)
		return true
	else
		dbg("Measurement at subpath '%s' is OLD (%d <= %d)", ctx.path, measurement_timestamp, state.last_timestamp or -1)
		return false
	end
end

---------------------------------------------------------------------------------------------------
-- build the intermediate representation
---------------------------------------------------------------------------------------------------

function ir_create_device(ctx)
	local device_id = get_device_id(ctx)

	if ir_buffer[device_id] then
		return ir_buffer[device_id]
	end

	local device_data = {
		colors = { }, -- traces, indexed by color
		auxiliary = { }, -- traces, indexed by _type
		id = device_id,
		host = ctx:get("device"),
		name = string.format("Marking unit %s on host %s", ctx:get("marker"), ctx:get("device"))
	}

	ir_buffer[device_id] = device_data

	return device_data
end

function ir_create_supply_in(ctx, tbl, index)
	local supply

	if tbl[index] then
		supply = tbl[index]
		assert(util.is_table(supply.data))
	else
		supply = { data = { } }
		tbl[index] = supply
	end

	util.assign_assert(supply, "id", ctx:get("stable_name"))
	util.assign_assert(supply, "type", ctx:get("type"))
	util.assign_assert(supply, "color", ctx:get("color"))
	util.assign_assert(supply, "description", ctx:get("description"))
	util.assign_assert(supply, "unit", ctx:get("value.unit"))
	util.assign_assert(supply, "max", assert(tonumber(ctx:get("max"))))

	return supply
end

function ir_create_supply(ctx, device)
	local supply_type = ctx:get("type")

	if supply_type == "toner" then
		return ir_create_supply_in(ctx, device.colors, ctx:get("stable_name"))
	else
		return ir_create_supply_in(ctx, device.auxiliary, ctx:get("stable_name"))
	end
end

function ir_create_usage(ctx, device)
	local usage

	if device.usage then
		usage = device.usage
		assert(util.is_table(usage.data))
	else
		usage = { data = { } }
		device.usage = usage
	end

	util.assign_assert(usage, "type", "usage")
	util.assign_assert(usage, "unit", ctx:get("usage.unit"))

	return usage
end

function ir_add_point_to(ctx, tbl, value_name)
	table.insert(tbl.data, {
		timestamp = assert(tonumber(ctx:get("timestamp"))) * 1000, -- plotly-specific: milliseconds since Epoch
		value = assert(tonumber(ctx:get(value_name)))
	})
end

function ir_add_supply_point(ctx)
	local device = ir_create_device(ctx)
	local supply = ir_create_supply(ctx, device)

	ir_add_point_to(ctx, supply, "value")
end

function ir_add_usage_point(ctx)
	if (ctx:get("usage")) then
		local device = ir_create_device(ctx)
		local usage = ir_create_usage(ctx, device)

		ir_add_point_to(ctx, usage, "usage")
	end
end

function dir_generate_context(root_ctx, path_pieces)
	-- generate context for the subpath, incrementally
	-- i. e. if we've found a/b/c/d, generate contexts for a, a/b, a/b/c and a/b/c/d
	-- and store them in the iteration state
	local last_ctx = root_ctx
	for i, piece in ipairs(path_pieces) do
		if not piece.ctx then
			-- parse the directory name
			local k, v = string.match(piece.key, "^([^=]+)=(.+)$")
			assert(k and v)

			piece.ctx = util.clone(last_ctx):descend(false, { k, v })
		end
		last_ctx = piece.ctx
	end

	return last_ctx
end

for subpath, mode, state in util.dir(ctx.root, true) do
	local name = util.last(state.pieces).key

	if mode == "directory" and string.match(name, "^supply=") then
		-- get context for the subpath
		subpath_ctx = dir_generate_context(ctx, state.pieces)

		-- do not descend further
		util.dir_prune(state)

		-- process the measurement
		ir_add_supply_point(subpath_ctx)
	elseif mode == "directory" and string.match(name, "^marker=") then
		-- get context for the subpath
		subpath_ctx = dir_generate_context(ctx, state.pieces)

		if ir_measurement_is_recent(subpath_ctx) then
			-- process the measurement and continue on to supplies
			ir_add_usage_point(subpath_ctx)
		else
			-- do not descend further
			util.dir_prune(state)
		end
	end
end

---------------------------------------------------------------------------------------------------
-- postprocess the built IR: sort and remove consecutive identical measurements
---------------------------------------------------------------------------------------------------

function ir_postprocess_trace(trace)
	table.sort(trace.data, function(a, b) return a.timestamp < b.timestamp end)
	-- TODO: remove consecutive identical measurements for traces with imprecise units
	-- (e. g. percent, but not impressions)

	if util.in_set(trace.unit, { "percent" }) then
		local last_v
		local new_data = { }
		for i, v in ipairs(trace.data) do
			local next_v = trace.data[i+1]

			if last_v and last_v.value == v.value and (next_v and next_v.value <= v.value) then
				-- remove consecutive identical measurements if they are expected to be imprecise/rounded,
				-- so we get a slope which spans over all the consecutive measurements instead of a sharp edge.
				-- however, do not do this if we are followed by a positive front (which, for supplies, would mean a refill),
				-- or if we are the last measurement in series.
				local function fmtd(arg) return os.date("%F %T", arg/1000) end
				dbg("last %s-> %s, removing %s -> %s, next %s -> %s",
				    fmtd(last_v.timestamp), last_v.value, fmtd(v.timestamp), v.value, next_v and fmtd(next_v.timestamp) or "<n/a>", next_v and next_v.value or "<n/a>")
			else
				table.insert(new_data, v)
				last_v = v
			end
		end

		assert(#new_data <= #trace.data)
		trace.data = new_data
	end

	--[[for i, v in pairs(trace.data) do
		v.timestamp = os.date("%F %T", v.timestamp)
	end]]
end

for i, device in pairs(ir_buffer) do
	for color, trace in pairs(device.colors) do
		ir_postprocess_trace(trace)
	end

	for id, trace in pairs(device.auxiliary) do
		ir_postprocess_trace(trace)
	end

	if device.usage then
		ir_postprocess_trace(device.usage)
	end
end

io.open("ir_buffer", "w"):write(dkjson.encode(ir_buffer, { indent = true }))

---------------------------------------------------------------------------------------------------
-- build JSON replies
---------------------------------------------------------------------------------------------------

function render_trace_color(color)
	if color then
		return assert (({
			black = "rgb(0, 0, 0)",
			cyan = "rgb(0, 255, 255)",
			magenta = "rgb(255, 0, 255)",
			yellow = "rgb(255, 255, 0)"
		})[color], string.format("Unknown toner color: '%s'", color))
	else
		return "rgb(128, 128, 128)"
	end
end

function render_trace_fill_color(color)
	if color then
		return assert (({
			black = "rgba(0, 0, 0, 0.1)",
			cyan = "rgba(0, 255, 255, 0.1)",
			magenta = "rgba(255, 0, 255, 0.1)",
			yellow = "rgba(255, 255, 0, 0.1)"
		})[color], string.format("Unknown toner color: '%s'", color))
	else
		return "rgba(128, 128, 128, 0.05)"
	end
end

function render_trace_name(ir_trace)
	if ir_trace.color then
		return string.format("%s %s (%s)", ir_trace.color, ir_trace.type, ir_trace.unit)
	else
		return string.format("%s (%s)", ir_trace.type, ir_trace.unit)
	end
end

function json_customize_y_axis_by_unit(json_axis, unit)
	util.table_join(json_axis, ({
		["percent"] = {
			ticksuffix = "%"
		}
	})[unit] or {
		exponentformat = "B"
	})

	json_axis.title = string.format("%s (%s)", json_axis.title, unit)
end

function json_customize_y_axis_by_range(json_axis, config_axis)
	if config_axis.max_unbounded then
		-- we have at least one unbounded trace on this axis, enable autorange mode
		json_axis.range = nil
		json_axis.autorange = true
		json_axis.fixedrange = true
	elseif config_axis.max then
		-- set range and disengage autorange left from initial axis creation
		json_axis.range = { 0, config_axis.max }
		json_axis.autorange = false
		json_axis.fixedrange = true
	end
end

function json_customize_x_axis(json_axis)
	util.table_join(json_axis, {
		autorange = true,
		fixedrange = false
	})
end

function bind_to_axis(json_output, config, axis_id, trace_id, properties)
	local config_axis = config.axes[axis_id]
	local config_trace = config.traces[trace_id]

	assert(config_axis) -- we don't support creating axes dynamically

	local json_axis

	if not config_axis.axis_created then
		-- create basic axis fields in the JSON
		json_axis = util.table_join(dkjson.decode(util.read_all(string.format("plotly-axis.json", axis_id))), config_axis.json_fields)

		-- customize axis parameters
		if properties.is_y then
			config_axis.unit = config_trace.unit
			json_customize_y_axis_by_unit(json_axis, config_axis.unit)
		else
			json_customize_x_axis(json_axis)
		end
	else
		if properties.is_y then
			assert(config_axis.unit == config_trace.unit, string.format("Axis '%s' changed unit from '%s' to '%s' (of trace '%s')", axis_id, config_axis.unit, config_trace.unit, trace_id))
		end

		json_axis = {
			type = config_axis.json_fields.type -- apparently needs to be re-specified each time
		}
	end

	if properties.is_y then
		-- accumulate maximums
		if config_trace.max then
			util.accumulate_max(config_axis, "max", config_trace.max)
			dbg("Axis '%s' now has maximum %d (trace '%s' has maximum %d)", axis_id, config_axis.max or 0, trace_id, config_trace.max or 0)
		else
			config_axis.max_unbounded = true
		end

		-- reconfigure axis range
		json_customize_y_axis_by_range(json_axis, config_axis)
	end

	-- assign the axis (actually update, not assign -- we can be called multiple times)
	if not json_output.layout[config_axis.key_in_json_layout] then
		json_output.layout[config_axis.key_in_json_layout] = json_axis
	else
		util.table_join(json_output.layout[config_axis.key_in_json_layout], json_axis)
	end

	-- actually bind the axis to the trace
	if not config_trace.trace_created then
		util.table_join(json_output.data[config_trace.index], config_axis.entry_in_json_trace)
	end

	-- mark axis as created
	config_axis.axis_created = true
end

function render_trace(json_output, config, template, trace_id, axes_ids, ir_trace)
	local config_trace = config.traces[trace_id]

	if not config_trace then
		-- allocate a new trace
		config_trace = {
			index = config.traces_last_index + 1
		}
		config.traces[trace_id] = config_trace
		config.traces_last_index = config_trace.index
	end

	local json_trace

	if not config_trace.trace_created then
		-- init trace, create basic trace fields in the JSON
		config_trace.name = render_trace_name(ir_trace)
		config_trace.unit = ir_trace.unit

		json_trace = util.table_join(dkjson.decode(util.read_all(string.format("plotly-%s-trace.json", template))), {
			name = config_trace.name,
			marker = {
				color = render_trace_color(ir_trace.color)
			},
			line = {
				color = render_trace_color(ir_trace.color)
			},
			fillcolor = render_trace_fill_color(ir_trace.color)
		})
	else
		assert(config_trace.unit == ir_trace.unit, "Trace '%s' changed unit from '%s' to '%s'", trace_id, config_trace.unit, ir_trace.unit)

		json_trace = {}
	end

	-- fill trace data points
	json_trace.x = util.table_map(ir_trace.data, "timestamp")
	json_trace.y = util.table_map(ir_trace.data, "value")

	-- accumulate maximums
	util.assign_assert(config_trace, "max", ir_trace.max)

	-- assign the trace
	json_output.data[config_trace.index] = json_trace

	-- bind the trace to axes (also adds the axes to the JSON output)
	bind_to_axis(json_output, config, axes_ids.x, trace_id, { })
	bind_to_axis(json_output, config, axes_ids.y, trace_id, { is_y = true })

	-- mark the trace as created
	config_trace.trace_created = true
end

function render_device(ir_device)
	local json_output = { data = { } }

	-- We keep our config in the JSON (and not in the disk format directly) to achieve transactional semantics
	-- by writing JSON back to disk only in case of successful send.
	local device_state = state_create_device(ir_device.id)
	local config = device_state.config

	if not config.plot_created then
		-- create basic layout of the plot
		json_output.layout = util.table_join(dkjson.decode(util.read_all("plotly-layout.json")), {
			title = string.format("Statistics for %s", ir_device.name)
		})
	else
		json_output.layout = { }
	end

	if not config.traces then
		-- preinit positions for well-known traces
		if  ir_device.colors["toner:cyan"]
		and ir_device.colors["toner:magenta"]
		and ir_device.colors["toner:yellow"]
		and ir_device.colors["toner:black"] then
			config.traces_last_index = 4
			config.traces = {
				["toner:cyan"] = {
					index = 1
				},
				["toner:magenta"] = {
					index = 2
				},
				["toner:yellow"] = {
					index = 3
				},
				["toner:black"] = {
					index = 4
				}
			}
		elseif ir_device.colors["toner:black"] then
			config.traces_last_index = 1
			config.traces = {
				["toner:black"] = {
					index = 1
				}
			}
		else
			config.traces_last_index = 0
			config.traces = { }
		end
	end

	if not config.axes then
		-- preinit axes
		config.axes = {
			["timestamp"] = {
				json_fields = {
					title = "Timestamp",
					type = "date",
					tickangle = -45
				},
				key_in_json_layout = "xaxis",
				entry_in_json_trace = { xaxis = "x" }
			},

			["supply_levels"] = {
				json_fields = {
					title = "Supply levels",
					type = "linear",
					side = "left"
				},
				key_in_json_layout = "yaxis",
				entry_in_json_trace = { yaxis = "y" }
			},

			["usage"] = {
				json_fields = {
					title = "Usage",
					type = "linear",
					side = "right",
					rangemode = "tozero",
					overlaying = "y"
				},
				key_in_json_layout = "yaxis2",
				entry_in_json_trace = { yaxis = "y2" }
			}
		}
	end

	for id, ir_trace in pairs(ir_device.colors) do
		-- process toner traces
		render_trace(json_output, config, "color", id, { x = "timestamp", y = "supply_levels" }, ir_trace)
	end

	for id, ir_trace in pairs(ir_device.auxiliary) do
		-- process other traces
		render_trace(json_output, config, "aux", id, { x = "timestamp", y = "supply_levels" }, ir_trace)
	end

	if ir_device.usage then
		-- process usage counter trace, if any
		render_trace(json_output, config, "usage", "usage", { x = "timestamp", y = "usage" }, ir_device.usage)
	end

	-- POST the json here
	local response = plotly_request("plot", json_output.data, {
		filename = "ASP/printers/" .. ir_device.id,
		fileopt = config.plot_created and "extend" or "overwrite",
		layout = json_output.layout,
		world_readable = true
	})

	if response then
		-- we've done, write back the config mapping
		config.plot_created = true
	else
		-- tell that we don't want to save changes in mappings
		state_reset_device(ir_device.id)
	end
end

function plotly_request(origin, args, kwargs)
	local username = state_ctx:get("username")
	local api_key = state_ctx:get("api_key")
	local data = { }

	table.insert(data, "un=" .. username)
	table.insert(data, "key=" .. api_key)
	table.insert(data, "platform=" .. "lua")
	table.insert(data, "origin=" .. origin)
	table.insert(data, "args=" .. dkjson.encode(args))
	table.insert(data, "kwargs=" .. dkjson.encode(kwargs))

	local body, code = http.request("http://plot.ly/clientresp", table.concat(data, "&"))

	if not body then
		log.E("Failed to POST, error: '%s'", tostring(code))
		return false, tostring(code)
	elseif code ~= 200 then
		log.E("Request failed, code %s, text follows:\n%s", tostring(code), tostring(body))
		return false, tostring(body)
	else
		local response = dkjson.decode(body)
		if not util.is_table(response) then
			log.E("Request failed, response is ill-formed, text follows:\n%s", tostring(body))
			return false, tostring(body)
		elseif response.error ~= "" then
			log.E("Request failed, response error field is non-empty, text follows:\n%s", tostring(body))
			return false, tostring(response.error)
		else
			log.N("Request OK, code %s, text follows:\n%s", tostring(code), tostring(body))
			return true
		end
	end
end

for i, ir_device in pairs(ir_buffer) do
	render_device(ir_device)
end

state_save_all()
