"use client";

import React, { useState, useEffect, useMemo, useCallback } from 'react';
import GridLayout, { Layout, verticalCompactor, useContainerWidth } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

interface DashboardGridProps {
  children: React.ReactNode;
  onLayoutChange?: (layout: Layout) => void;
  cols?: number;
  rowHeight?: number;
  containerPadding?: [number, number];
}

const DashboardGrid: React.FC<DashboardGridProps> = ({
  children,
  onLayoutChange,
  cols = 12,
  rowHeight = 60,
  containerPadding = [20, 20],
}) => {
  const [layout, setLayout] = useState<Layout>([]);
  const { width, containerRef } = useContainerWidth();

  const childrenArray = useMemo(() => React.Children.toArray(children), [children]);

  useEffect(() => {
    const saved = localStorage.getItem('dashboard-layout');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length > 0) {
          // eslint-disable-next-line react-hooks/set-state-in-effect
          setLayout(parsed);
          return;
        }
      } catch {
        /* ignore */
      }
    }
    const defaultLayout = childrenArray.map((child, i) => {
      const element = child as React.ReactElement<{ id?: string }>;
      const id = element?.props?.id || `card-${i}`;
      return {
        i: id,
        x: (i * 4) % cols,
        y: Math.floor(i / (cols / 4)) * 2,
        w: 6,
        h: 6,
        minW: 4,
        minH: 4,
      };
    });
    setLayout(defaultLayout);
  }, [childrenArray, cols]);

  const handleLayoutChange = useCallback((newLayout: Layout) => {
    setLayout(newLayout);
    localStorage.setItem('dashboard-layout', JSON.stringify(newLayout));
    if (onLayoutChange) onLayoutChange(newLayout);
  }, [onLayoutChange]);

  return (
    <div ref={containerRef} className="w-full">
      <GridLayout
        className="layout"
        layout={layout}
        onLayoutChange={handleLayoutChange}
        width={width}
        gridConfig={{ cols, rowHeight, containerPadding }}
        dragConfig={{ enabled: true, handle: '.drag-handle' }}
        resizeConfig={{ enabled: true }}
        compactor={verticalCompactor}
        autoSize
      >
        {childrenArray.map((child, idx) => {
          if (!React.isValidElement(child)) return null;
          const element = child as React.ReactElement<{ id?: string }>;
          const id = element?.props?.id || `card-${idx}`;
          return (
            <div key={id} data-grid={{ i: id }}>
              {child}
            </div>
          );
        })}
      </GridLayout>
    </div>
  );
};

export default DashboardGrid;
