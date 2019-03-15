local function equalBytes(a, b)
    if a == nil and b == nil then
        return true
    else
        if a == nil or b == nil then
            return false
        end
    end

    if bytes.get_string(a, 1, bytes.size(a)) == bytes.get_string(b, 1, bytes.size(b)) then
        return true
    else
        return false
    end
end

local function containsAllEntries(a, b)
    for key, value in map.pairs(b) do
        if(a == nil and value == nil or equalBytes(a[key], value)) then
        else
            return false
        end
    end
    return true
end

function check_and_lock(rec, transaction, expected_values_map)

    if aerospike:exists(rec) then
        local current_transaction = rec['transaction']

        if current_transaction then
            if equalBytes(current_transaction, transaction) then
                return 0
            else
                return 1
            end
        end

        if not containsAllEntries(rec['entries'], expected_values_map) then
            return 2
        end

        rec['transaction'] = transaction;
        aerospike:update(rec)
        return 0
    else
        rec['lock_time'] = current_time;
        aerospike:create(rec)
        return 0
    end
end