"use client";

import * as echarts from 'echarts';
import { useEffect, useRef } from 'react';

function randomOHLC(basePrice: number, volatility: number) {
  const open = basePrice;
  const close = open + (Math.random() - 0.5) * volatility;
  const high = Math.max(open, close) + Math.random() * volatility * 0.5;
  const low = Math.min(open, close) - Math.random() * volatility * 0.5;
  return { open, close, high, low, closePrice: close };
}

export default function Chart() {
  const chartRef = useRef<HTMLDivElement>(null);
  const instanceRef = useRef<echarts.ECharts | null>(null);
  const dataRef = useRef<number[][]>([]);
  const userZoomedRef = useRef(false);

  useEffect(() => {
    if (!chartRef.current) return;
    const chart = echarts.init(chartRef.current, 'dark');
    instanceRef.current = chart;

    let basePrice = 100;
    const data: number[][] = [];

    chart.on('dataZoom', () => {
      userZoomedRef.current = true;
    });

    const windowSize = 80;

    const update = () => {
      const { open, close, high, low } = randomOHLC(basePrice, 6);
      data.push([open, close, low, high]);
      basePrice = close;

      const opt: any = {
        grid: { top: 8, bottom: 24, left: 8, right: 60 },
        xAxis: {
          type: 'category',
          show: true,
          axisLine: { show: true },
          axisTick: { show: true },
          splitLine: { show: false },
          axisLabel: { fontSize: 10 },
        },
        yAxis: {
          type: 'value',
          show: true,
          position: 'right',
          splitLine: { show: true, lineStyle: { color: '#333', type: 'dashed' } },
          axisLabel: { fontSize: 10, formatter: (v: any) => v.toFixed(2) },
        },
        series: [{
          type: 'candlestick', data, animation: false,
          markLine: {
            silent: true,
            symbol: 'none',
            lineStyle: { color: '#e0e0e0', width: 1 },
            label: {
              show: true,
              formatter: () => basePrice.toFixed(2),
              position: 'end',
              backgroundColor: '#e0e0e0',
              color: '#000',
              padding: [2, 6],
              borderRadius: 2,
              fontSize: 11,
            },
            data: [{ yAxis: basePrice }],
          },
        }],
      };

      if (!userZoomedRef.current) {
        const endVal = data.length - 1;
        const startVal = Math.max(0, endVal - windowSize + 1);
        opt.dataZoom = [
          { type: 'inside', xAxisIndex: [0], startValue: startVal, endValue: endVal },
          { type: 'inside', yAxisIndex: [0] },
        ];
      }

      chart.setOption(opt);
    };

    update();
    const interval = setInterval(update, 1000);

    const resize = () => chart.resize();
    window.addEventListener('resize', resize);

    return () => {
      clearInterval(interval);
      window.removeEventListener('resize', resize);
      chart.dispose();
    };
  }, []);

  return <div ref={chartRef} className="h-[400px] w-[600px]" />;
}



