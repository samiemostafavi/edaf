def process_ul_nlmt(lines):
    # lines = lines.splitlines()
    # Find the index of the line containing [CloseConn]
    close_conn_index = None
    for i, line in enumerate(lines):
        if '[CloseConn]' in line:
            close_conn_index = i
            break

    # If [CloseConn] is not found, return an empty list
    if close_conn_index is not None:
        filteredlines = lines[0:close_conn_index]
    else:
        filteredlines = lines

    parsed_logs = []
    source_ip = None
    # Process the lines after [CloseConn]
    for line in filteredlines:
        # Separate the line by white space
        line_parts = line.split()
        source_ip = line_parts[0][1:-1]

        # Create a dictionary for each line
        log_dict = {"source": source_ip}
        for part in line_parts[1:]:
            key, value = part.split('=')
            log_dict[key] = value

        parsed_logs.append(log_dict)

    return parsed_logs