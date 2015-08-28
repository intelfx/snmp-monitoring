lfs = require("lfs")

util = require("util")
db = require("db")
log = require("log")

function postprocess_snmp(name, type, value)
	if type == "INTEGER" then
		value = string.match(value, "(.*)%([0-9]+%)") or value
	elseif type == "STRING" then
		value = string.match(value, "\"(.*)\"") or value
	end

	return name, type, value
end

function parse_snmp_iter(state)
	local line = state.iter(state.state)

	if not line then
		dbg("called, got nil line")
		return nil
	end

	dbg("called, got line '%s'", line)

	if state.xfrm then
		line = state.xfrm(line)
		dbg("transformed line: '%s'", line)
	end

	local response = {}

	response = { string.match(line, "^= No Such Instance ") } -- nothing useful
	if response[1] then
		return nil -- single instance "No Such Instance" response
	end

	response = { string.match(line, "^= (.+): (.+)$") } -- type, value
	if response[1] then
		return postprocess_snmp("", unpack(response)) -- single instance (get-style) response
	end

	response = { string.match(line, "^(.+) = (.+): (.+)$") } -- name, type, value
	if response[1] then
		return postprocess_snmp(unpack(response)) -- normal response
	end

	log.W("bad line: '%s'", line)
	return nil -- bad response
end

-- arg prog: string
--     command to launch, without community, device and OID path
-- arg device: table { host, community, index }
--     information about device: host name, community (which will be appended to command line via "-c") and index (which will be substituted in arg mib by string.format())
-- arg mib: string
--     OID path or MIB name to lookup
--     first specifier must be %s and will be replaced with value of arg device.index
--     next specifiers will be replaced with varargs
function parse_snmp(prog, device, mib, ...)
	mib = string.format(mib, device.index, ...)
	local file = io.popen(string.format("%s -c %s %s %s", prog, device.community, device.host, mib))
	local state = {}
	local idx

	state.xfrm = function(line) return string.sub(line, #mib + 2) end -- cut the MIB name plus dot from response lines, so only subpath remains

	state.iter, state.state, idx = file:lines() -- inner iterator

	return parse_snmp_iter, state, idx
end

function walk_snmp(device, mib, ...)
	return parse_snmp("snmpwalk -Cc -m ALL -v 2c", device, mib, ...)
end

function get_snmp(device, mib, ...)
	for name, type, value in parse_snmp("snmpget -m ALL -v 2c", device, mib, ...) do
		return value
	end
end

-- parse args

f, s, i = ipairs(arg)
i, v = f(s, i)

assert(i, "Database root not given")
db_root = v
log.N("database root is '%s'", db_root)

devices = {}
for i, v in f, s, i do
	log.N("device '%s', assuming SNMP index 1, assuming community public", v)
	table.insert(devices, { host = v, index = 1, community = "public" })
end

function run_snmp_walk(host)
	return string.format("snmpwalk ")
end

function mib_append(mib, index)
	return mib .. "." .. index
end

function process_supply(ctx, device)
	local supply = ctx:get("supply")

	ctx:set("_type",       get_snmp(device, "Printer-MIB::prtMarkerSuppliesType.%s.%s", supply))
	ctx:set("description", get_snmp(device, "Printer-MIB::prtMarkerSuppliesDescription.%s.%s", supply))
	ctx:set("value",       get_snmp(device, "Printer-MIB::prtMarkerSuppliesLevel.%s.%s", supply))
	ctx:set("max",         get_snmp(device, "Printer-MIB::prtMarkerSuppliesMaxCapacity.%s.%s", supply))
	ctx:set("value.unit",  get_snmp(device, "Printer-MIB::prtMarkerSuppliesSupplyUnit.%s.%s", supply))

	local colorant = get_snmp(device, "Printer-MIB::prtMarkerSuppliesColorantIndex.%s.%s", supply)
	if colorant and colorant ~= "0" then
		ctx:set("color", get_snmp(device, "Printer-MIB::prtMarkerColorantValue.%s.%s", colorant))
	end

	if ctx:get("color") then
		ctx:set("name", string.format("%s %s (%s)", ctx:get("color"), ctx:get("_type"), ctx:get("value.unit")))
	else
		ctx:set("name", string.format("%s (%s)", ctx:get("_type"), ctx:get("value.unit")))
	end
end

function process_marker(ctx, device)
	-- find those supplies which are 1) consumable and 2) of our marker
	local current_supplies = {}
	for name, type, value in walk_snmp(device, "Printer-MIB::prtMarkerSuppliesMarkerIndex.%s") do
		dbg("supply %s belongs to marker %s", name, value)
		if device.supplies[name] and value == ctx:get("marker") then
			table.insert(current_supplies, name)
		end
	end

	-- process each found supply
	local supply_ctx
	for i, supply in pairs(current_supplies) do
		supply_ctx = util.clone(ctx):descend(false, { "supply", supply })
		process_supply(supply_ctx, device)
	end

	-- process usage count, if any
	local usage = get_snmp(device, "Printer-MIB::prtMarkerLifeCount.%s.%s", marker)
	local usage_hp = get_snmp(device, "HP-LASERJET-COMMON-MIB::total-engine-page-count.0")
	if usage then
		ctx:set("usage", usage)
		ctx:set("usage.unit", get_snmp(device, "Printer-MIB::prtMarkerCounterUnit.%s.%s", marker))
	elseif usage_hp then
		ctx:set("usage", usage_hp)
		ctx:set("usage.unit", "impressions")
	end
end

function process_device(ctx, device)
	device.supplies = {}

	-- parse consumable supplies
	for name, type, value in walk_snmp(device, "Printer-MIB::prtMarkerSuppliesClass.%s") do
		dbg("got supply: '%s' type '%s' value '%s'", name, type, value)

		if type == "INTEGER" and value == "supplyThatIsConsumed" then
			dbg("supply is consumed")
			device.supplies[name] = true
		end
	end

	-- parse available markers in the device (FIXME: now we consider only the default marker)
	device.markers = { get_snmp(device, "Printer-MIB::prtMarkerDefaultIndex.%s") }
	dbg("got default marker: '%s'", device.markers[1])

	-- process each available marker
	for k, marker in pairs(device.markers) do
		ctx_marker = util.clone(ctx):descend(true, { "marker", marker })
		process_marker(ctx_marker, device)
	end
end

timestamp = os.time()
measurement_label = os.date("%F@%H", timestamp)
log.N("measurement began at %s", os.date("%F %H:%M:%S", timestamp))
log.N("measurement label is %s", measurement_label)

ctx = db.new(db_root)
ctx:descend (false, { "time", measurement_label })

for i, device in pairs(devices) do
	ctx_device = util.clone(ctx):descend(false, { "device", device.host })

	if ctx_device.entries["timestamp"] then
		log.N("measurement for device %s label %s already exists, skipping", device.host, measurement_label)
	else
		ctx_device:set("timestamp", tostring(timestamp))
		process_device(ctx_device, device)
	end
end
