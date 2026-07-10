"use client";

import React, { useState, useEffect, useMemo } from 'react';
import GridLayout, { Layout, verticalCompactor, useContainerWidth } from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import 'react-resizable/css/styles.css';

interface DashboardGridProps {
  children: React.ReactNode; // can be a single child or array
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

  // Convert children to array for easier mapping
  const childrenArray = useMemo(() => React.Children.toArray(children), [children]);

  // Load saved layout from localStorage on mount
  useEffect(() => {
    const saved = localStorage.getItem('dashboard-layout');
    if (saved) {
      try {
        const parsed = JSON.parse(saved);
        if (Array.isArray(parsed) && parsed.length > 0) {
          setLayout(parsed);
          return;
        }
      } catch {}
    }
    // Generate default layout based on children count
    const defaultLayout = childrenArray.map((child, i) => {
      const id = (child as any)?.props?.id || `card-${i}`;
      return {
        i: id,
        x: (i * 4) % cols,
        y: Math.floor(i / (cols / 4)) * 2,
        w: 4,
        h: 4,
        minW: 2,
        minH: 2,
      };
    });
    setLayout(defaultLayout);
  }, [childrenArray, cols]);

  const handleLayoutChange = (newLayout: Layout) => {
    setLayout(newLayout);
    localStorage.setItem('dashboard-layout', JSON.stringify(newLayout));
    if (onLayoutChange) onLayoutChange(newLayout);
  };

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
        {childrenArray.map((child) => {
          if (!React.isValidElement(child)) return null;
          const id = (child as any)?.props?.id || `card-${Math.random()}`;
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