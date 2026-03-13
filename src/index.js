/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */
import Papa from 'papaparse'
import * as d3 from 'd3'

let mainData = [];
let stackedData = [];
let filteredData = [];
let kpi = {
	bebanAvg: 0,
	suhuAvg: 0,
	humidityAvg: 0,
	correlation: 0,
	maxEntry: null,
	maxDate: null,
	maxCondition: null
};
async function loadData() {
	const mainDataURL = 'https://raw.githubusercontent.com/adriann03/EDA_Kelompok06_CuacaEnergi/refs/heads/main/data/processed/energy_weather_wide_final.csv';
	const stackedDataURL = 'https://raw.githubusercontent.com/adriann03/EDA_Kelompok06_CuacaEnergi/refs/heads/main/data/processed/energy_generation_long_melted.csv';
	
	const [mainRes, stackedRes] = await Promise.all([
		fetch(mainDataURL),
		fetch(stackedDataURL)
	]);
	const [mainText, stackedText] = await Promise.all([
		mainRes.text(),
		stackedRes.text()
	]);

	mainData = Papa.parse(mainText, { header: true, skipEmptyLines: true })
				.data.filter(r => r.Beban_Puncak_Aktual?.trim());
	stackedData = Papa.parse(stackedText, { header: true, skipEmptyLines: true })
					.data.filter(r => r.Sumber_Energi?.trim());
	filteredData = mainData.slice();
}
function preprocessData() {
	const chunkSize = 1000;
	let processed = 0;
	
	function processChunk() {
		const end = Math.min(processed + chunkSize, mainData.length);
		
		for(let i = processed; i < end; i++) {
			const d = mainData[i];
			mainData[i] = {
				...d,
				Beban_Puncak_Aktual: +d.Beban_Puncak_Aktual || 0,
				tempK: +d.temp || NaN,
				tempC: isNaN(+d.temp) ? NaN : Math.round((+d.temp - 273.15) * 10) / 10,
				humidity: +d.humidity || NaN,
				rain_1h: +(d.rain_1h || 0) || 0,
				Waktu_Date: new Date(d.Waktu_UTC)
			};
		}
		processed = end;
		if (processed < mainData.length) {
			setTimeout(processChunk, 0);
		} else {
			processStackedData();
		}
	}
	
	function processStackedData() {
		stackedData = stackedData.map(d => ({
			...d,
			Kapasitas_Generasi: +d.Kapasitas_Generasi || 0,
			Beban_Puncak_Aktual: +d.Beban_Puncak_Aktual || 0
		}));
	}
	
	processChunk();
}
function calculateCorrelation(x, y) {
	const n = x.length;
	if (n === 0) return 0;
	
	const sum_x = d3.sum(x);
	const sum_y = d3.sum(y);
	const sum_xy = d3.sum(x.map((xi, i) => xi * y[i]));
	const sum_x2 = d3.sum(x.map(xi => xi * xi));
	const sum_y2 = d3.sum(y.map(yi => yi * yi));
	
	const numerator = n * sum_xy - sum_x * sum_y;
	const denominator = Math.sqrt((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y));
	
	return denominator === 0 ? 0 : numerator / denominator;
}
function kelvinToCelsius(kelvin) {
	const k = parseFloat(kelvin);
	if (isNaN(k)) return NaN;
	return Math.round((k - 273.15) * 10) / 10;
}
function updateKPIs() {
	if (filteredData.length === 0) return;
	
	kpi.bebanAvg = d3.mean(filteredData, d => parseFloat(d.Beban_Puncak_Aktual));
	kpi.suhuAvg = d3.mean(filteredData, d => kelvinToCelsius(parseFloat(d.temp)));
	kpi.humidityAvg = d3.mean(filteredData, d => parseFloat(d.humidity));
	kpi.correlation = calculateCorrelation(
		filteredData.map(d => kelvinToCelsius(parseFloat(d.temp))),
		filteredData.map(d => parseFloat(d.Beban_Puncak_Aktual))
	);
	kpi.maxEntry = filteredData.reduce((max, d) => 
		parseFloat(d.Beban_Puncak_Aktual) > parseFloat(max.Beban_Puncak_Aktual) ? d : max
	);
	kpi.maxDate = kpi.maxEntry.Waktu_UTC.split(' ')[0];
}

export default {
	async fetch(request, env, ctx) {
		if (mainData.length === 0) await loadData();
		preprocessData();
		updateKPIs();
		return new Response(JSON.stringify(kpi), {
			headers: { 'Content-Type': 'application/json' },
		});
	},
};
