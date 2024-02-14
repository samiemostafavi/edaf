from loguru import logger

def get_lines_containing_s(inp_lines):
        return [line for line in inp_lines if ' S ' in line]

class rdtsctotsOnline():

    def __init__(self,name) -> None:
        self.name = name
        self.s_lines = list()
        self.offset_cpufreq = None

    def _update_offset_cpufreq(self):
        first_S = self.s_lines[0].split()
        last_S = self.s_lines[-1].split()
        cycle_offset = int(first_S[0])
        time_offset = float(first_S[3])
        cpufreq = int((int(last_S[0]) - int(first_S[0]))/(float(last_S[3]) - float(first_S[3])))
        self.offset_cpufreq = (cycle_offset, time_offset, cpufreq)
    
    def return_rdtsctots(self, lines : str):
        # split and filter input lines
        #lines = unsplitted_lines.splitlines()
        lines = [line for line in lines if not line.startswith('#')]

        # get S lines
        new_slines = get_lines_containing_s(lines)
        if len(new_slines) > 0:
            self.s_lines = [ *self.s_lines, *new_slines ]
            # remove duplicates
            self.s_lines = list(dict.fromkeys(self.s_lines))

        # check we have minimum 2 s lines
        if len(self.s_lines) < 2:
            logger.warning("Waiting for CPU frequency info...")
            return []
        
        self._update_offset_cpufreq()

        # process the lines
        offset, time0, cpufreq = self.offset_cpufreq
        newlines = []
        for l in lines:  # Compute and replace rdtsc value by gettimeofday
            tmp = l.split(" ", 1)
            if len(tmp) > 1:
                if tmp[0].isnumeric():
                    newlines.append(f"%.6f {tmp[1]}" % ((int(tmp[0]) - offset)/cpufreq + time0))
                else:
                    logger.warning(f"non-numeric first element in {self.name} lseq: {l}")
            else:
                logger.warning(f"unusual line in {self.name} lseq: {l}")
        return sorted(newlines)
