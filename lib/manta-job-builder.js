/*
 * lib/manta-job-builder.js: helper class for constructing a Manta job
 */

var mod_assertplus = require('assert-plus');

/* Public interface */
module.exports = MantaJobBuilder;

function MantaJobBuilder()
{
	this.jb_name = '';
	this.jb_phases = [];
}

/*
 * Set the name of the job.
 */
MantaJobBuilder.prototype.name = function (name)
{
	mod_assertplus.string(name, 'name');
	this.jb_name = name;
};

/*
 * Add a phase to the job.
 */
MantaJobBuilder.prototype.phase = function (phase)
{
	mod_assertplus.object(phase, 'phase');
	mod_assertplus.string(phase.type, 'phase.type');
	mod_assertplus.optionalArrayOfString(phase.assets, 'phase.assets');
	mod_assertplus.optionalString(phase.init, 'phase.init');
	mod_assertplus.string(phase.exec, 'phase.exec');
	this.jb_phases.push(phase);
};

/*
 * Return the job definition, as a JS object.
 */
MantaJobBuilder.prototype.job = function ()
{
	return ({
	    'name': this.jb_name,
	    'phases': this.jb_phases
	});
};

/*
 * Return the job definition as JSON text.
 */
MantaJobBuilder.prototype.json = function (pretty)
{
	return (JSON.stringify(this.job(), null, pretty ? '    ' : ''));
};
