var readline = require('readline');

var read_timeseries = function(callback)
{
    var rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: false
    });

    // All timeseries
    var ts = [];

    // Currently being-built timeseries
    var curts = {};

    var state = 0;
    rl.on('line', function(line)
    {
        if(state == 0)
        {
            var array = line.split(" ");
            if(array.length != 2)
            {
                console.error("Fatal Error: Invalid formatted job-file");
                process.exit(1);
            }
            curts.tag = array[0];
            curts.uid = array[1];
        }
        else if(state == 1)
        {
            curts.ret_time = line;
        }
        else
        {
            curts.abs_time = line;
            //console.log(curts);
            ts.push(curts);
            curts = {};
        }
        
        state = (state + 1) % 3;
    });

    rl.on('close', function()
    {
        var is_empty = function(obj)
        {
            return Object.keys(obj).length === 0 && obj.constructor === Object
        }
        if(is_empty(curts) == false)
        {
            console.error("Fatal Error: Invalid formatted job-file");
            process.exit(1);
        }
        callback(ts);
    });
}

var options = require('commander');

function increaser(v, total) { return total + 1; };

options
  .version('0.0.1')
  .usage('[options]')
  //.option('-v, --verbose', 'Print more information', increaser, 0)
  //.option('-,--', '')
  .option('-,--', 'Outputting:')
  .option('-o, --output <prefix>', 'Append prefix output file name (default="")')
  .option('-O, --output_dir <folder>', 'Output to folder/{qry,ref}.jobfile (default=pwd)')
  .option('-,--', '')
  .option('-,--', 'Technique:')
  .option('-c, --cross_validation <n>', 'Do cross-validation splitting with "n" runs', parseInt)
  .option('-p, --percent <n>', 'Do precentage splitting ("n" percent to query, "1-n" to reference)', parseFloat)
  .option('-C, --count', 'Do counting of samples (# per tag)')
  .option('-,--', '')
  .option('-,--', 'Configuration:')
  .option('-r, --random', 'Utilize randomized splitting, instead of deterministic')
  .option('-b, --barrier', 'Enable tag-based barriers (i.e. keep proportions per tag)')
  .option('-h, --help', '');

// Capture the internal helper
var internal_help = options.help;

// Parse argv
options.parse(process.argv);

// Utilize our modified helper
var help = function()
{
    internal_help.bind(options)(function(value)
    {
        var help = value.split('\n');
        // Find our marker and use it to create categories
        var new_help = help.map(function(line)
        {
            var marker = line.indexOf("-,--");
            if(marker != -1)
            {
                return "   " + line.substr(marker+4).trim();
            }
            return line;
        }).filter(function(line)
        {
            return line.indexOf("-h, --help") == -1;
        });
        //console.log(new_help);

        return new_help.join('\n');
    });
}

// Was -h, or --help passed?
if(options.help == undefined)
    help();

// Not made yet
if(options.cross_validation)
{
    console.error("Fatal Error: Cross validation is not supported yet!");
    process.exit(1);
}
// Ww cannot do both
if(options.cross_validation && options.percent)
{
    console.error("Fatal Error: Cannot do both cross validation and percentage splitting at once");
    process.exit(1);
}
if(options.percent == 0 || options.percent == 1)
{
    console.error("Fatal Error: Cannot do 0% or 100% splitting");
    process.exit(1);
}

/**
 * Shuffles array in place. ES6 version
 * @param {Array} a items The array containing the items.
 */
function shuffle(a) {
    for (let i = a.length; i; i--) {
        let j = Math.floor(Math.random() * i);
        [a[i - 1], a[j]] = [a[j], a[i - 1]];
    }
}

// Process our input, into JSON
read_timeseries(function(ts)
{
    // Generate count-table
    var count_table = {};
    ts.forEach(function(timeseries)
    {
        count_table[timeseries.tag] = (count_table[timeseries.tag] || 0) + 1;
    });

    if(options.count)
    {
        console.log(count_table);
    }

    if(options.percent)
    {
        // Split data using barriers
        var barrier_gen = function()
        {
            // Bundle / barrier time-series by tag
            var tagged_ts = Object.keys(count_table).map(function(site)
            {
                return ts.filter(function(timeseries)
                {
                    return timeseries.tag == site;
                });
            });
            // Shuffle if we were asked to do random
            if(options.random)
            {
                tagged_ts.forEach(function(arr)
                {
                    shuffle(arr);
                });
            }
            // Pull out query-set
            var query_ts = tagged_ts.map(function(arr)
            {
                var query_elements = Math.floor(arr.length * options.percent);
                return arr.splice(0, query_elements);
            });
            // Pull out reference-set
            var reference_ts = tagged_ts.map(function(arr)
            {
                return arr;
            });
            // Flatten the sets
            var query_set = [].concat.apply([], query_ts);
            var reference_set = [].concat.apply([], reference_ts);
            // Return the data
            return {query: query_set, reference: reference_set};
        }

        // Split data without using barriers
        var non_barrier_gen = function()
        {
            // No barriers + random = pre-shuffle original data
            if(options.random)
                shuffle(ts);
            // Generate sets, by slicing the input in two pieces
            var query_elements = Math.floor(ts.length * options.percent);

            var query_set = ts.splice(0, query_elements);
            var reference_set = ts;
            // Return the data
            return {query: query_set, reference: reference_set};
        }
        
        // Acquire our splitted data
        var output = (options.barrier) ? barrier_gen() : non_barrier_gen();

        // Output split percentage
        var query_length = output.query.length;
        var ref_length = output.reference.length;
        var split_percentage = query_length / (query_length + ref_length);
        console.log("Actual percentage:", Math.floor(split_percentage*100) / 100);

        // Write output files
        var write_jobfile = function(path, ts)
        {
            var fs = require('fs');
            // Prepare output file
            var file = fs.createWriteStream(path);
            file.on('error', function(err)
            {
                console.error("Fatal Error: Unable to open output file:", path);
                console.error(err);
                process.exit(1);
            });
            // Write all the time series
            ts.forEach(function(timeseries)
            {
                file.write(timeseries.tag + " " + timeseries.uid + "\n");
                file.write(timeseries.ret_time + "\n");
                file.write(timeseries.abs_time + "\n");
            });
            // Close the file
            file.end();
        }
        // Load defaults for output_dir and output
        options.output_dir = (options.output_dir || '.');
        options.output = (options.output || '');
        // Write query and reference
        write_jobfile(options.output_dir + "/" + options.output + "qry.jobfile", output.query);
        write_jobfile(options.output_dir + "/" + options.output + "ref.jobfile", output.reference);
    }
});
